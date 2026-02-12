"""
RiskBricks Agent Base - Official Databricks ResponsesAgent Pattern

This module implements the base agent class using MLflow's ResponsesAgent,
following the exact pattern from Databricks AI Playground export.

Based on: Databricks "Get Started with AI Agents" training course
"""

import json
from typing import Any, Callable, Generator, Optional
from uuid import uuid4
import warnings

import mlflow
import openai
from databricks.sdk import WorkspaceClient
from databricks_openai import UCFunctionToolkit
from mlflow.entities import SpanType
from mlflow.pyfunc import ResponsesAgent
from mlflow.types.responses import (
    ResponsesAgentRequest,
    ResponsesAgentResponse,
    ResponsesAgentStreamEvent,
    output_to_responses_items_stream,
    to_chat_completions_input,
)
from openai import OpenAI
from pydantic import BaseModel
from unitycatalog.ai.core.base import get_uc_function_client


class ToolInfo(BaseModel):
    """
    Class representing a tool for the agent.
    - "name" (str): The name of the tool.
    - "spec" (dict): JSON description of the tool (matches OpenAI Responses format)
    - "exec_fn" (Callable): Function that implements the tool logic
    """
    name: str
    spec: dict
    exec_fn: Callable


def create_tool_info(tool_spec, exec_fn_param: Optional[Callable] = None, uc_function_client=None):
    """
    Create a ToolInfo object from a tool specification.
    
    Args:
        tool_spec: Tool specification from UCFunctionToolkit
        exec_fn_param: Optional custom execution function
        uc_function_client: UC function execution client
    
    Returns:
        ToolInfo object
    """
    # Remove 'strict' if present
    tool_spec["function"].pop("strict", None)
    tool_name = tool_spec["function"]["name"]
    udf_name = tool_name.replace("__", ".")

    # Define a wrapper that accepts kwargs for the UC tool call,
    # then passes them to the UC tool execution client
    def exec_fn(**kwargs):
        function_result = uc_function_client.execute_function(udf_name, kwargs)
        if function_result.error is not None:
            return function_result.error
        else:
            return function_result.value
    
    return ToolInfo(name=tool_name, spec=tool_spec, exec_fn=exec_fn_param or exec_fn)


class RiskBricksAgent(ResponsesAgent):
    """
    RiskBricks Agent - Tool-calling agent using Databricks Foundation Models
    
    This agent uses the official Databricks pattern:
    - MLflow ResponsesAgent base class
    - databricks-openai UCFunctionToolkit for UC tools
    - OpenAI SDK with Databricks endpoints
    - Proper tracing and error handling
    """

    def __init__(self, llm_endpoint: str, tools: list[ToolInfo], system_prompt: str = ""):
        """
        Initialize the RiskBricks Agent.
        
        Args:
            llm_endpoint: Name of the Databricks Foundation Model endpoint
            tools: List of ToolInfo objects
            system_prompt: System prompt for the agent
        """
        self.llm_endpoint = llm_endpoint
        self.system_prompt = system_prompt
        self.workspace_client = WorkspaceClient()
        self.model_serving_client: OpenAI = (
            self.workspace_client.serving_endpoints.get_open_ai_client()
        )
        self._tools_dict = {tool.name: tool for tool in tools}

    def get_tool_specs(self) -> list[dict]:
        """Returns tool specifications in the format OpenAI expects."""
        return [tool_info.spec for tool_info in self._tools_dict.values()]

    @mlflow.trace(span_type=SpanType.TOOL)
    def execute_tool(self, tool_name: str, args: dict) -> Any:
        """Executes the specified tool with the given arguments."""
        return self._tools_dict[tool_name].exec_fn(**args)

    def call_llm(self, messages: list[dict[str, Any]]) -> Generator[dict[str, Any], None, None]:
        """
        Call the LLM with messages and tools.
        
        Args:
            messages: List of message dictionaries
            
        Yields:
            Response chunks from the LLM
        """
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", message="PydanticSerializationUnexpectedValue")
            for chunk in self.model_serving_client.chat.completions.create(
                model=self.llm_endpoint,
                messages=to_chat_completions_input(messages),
                tools=self.get_tool_specs(),
                stream=True,
            ):
                chunk_dict = chunk.to_dict()
                if len(chunk_dict.get("choices", [])) > 0:
                    yield chunk_dict

    def handle_tool_call(
        self,
        tool_call: dict[str, Any],
        messages: list[dict[str, Any]],
    ) -> ResponsesAgentStreamEvent:
        """
        Execute tool calls, add them to the running message history, 
        and return a ResponsesStreamEvent w/ tool output
        
        Args:
            tool_call: Tool call dictionary
            messages: Message history
            
        Returns:
            ResponsesAgentStreamEvent with tool output
        """
        args = json.loads(tool_call["arguments"])
        result = str(self.execute_tool(tool_name=tool_call["name"], args=args))

        tool_call_output = self.create_function_call_output_item(tool_call["call_id"], result)
        messages.append(tool_call_output)
        return ResponsesAgentStreamEvent(type="response.output_item.done", item=tool_call_output)

    def call_and_run_tools(
        self,
        messages: list[dict[str, Any]],
        max_iter: int = 10,
    ) -> Generator[ResponsesAgentStreamEvent, None, None]:
        """
        Main agent loop: call LLM, execute tools, repeat until done.
        
        Args:
            messages: Initial messages
            max_iter: Maximum iterations to prevent infinite loops
            
        Yields:
            ResponsesAgentStreamEvent objects
        """
        for _ in range(max_iter):
            last_msg = messages[-1]
            if last_msg.get("role", None) == "assistant":
                return
            elif last_msg.get("type", None) == "function_call":
                yield self.handle_tool_call(last_msg, messages)
            else:
                yield from output_to_responses_items_stream(
                    chunks=self.call_llm(messages), aggregator=messages
                )

        yield ResponsesAgentStreamEvent(
            type="response.output_item.done",
            item=self.create_text_output_item("Max iterations reached. Stopping.", str(uuid4())),
        )

    def predict(self, request: ResponsesAgentRequest) -> ResponsesAgentResponse:
        """
        Non-streaming prediction.
        
        Args:
            request: ResponsesAgentRequest
            
        Returns:
            ResponsesAgentResponse
        """
        outputs = [
            event.item
            for event in self.predict_stream(request)
            if event.type == "response.output_item.done"
        ]
        return ResponsesAgentResponse(output=outputs, custom_outputs=request.custom_inputs)

    def predict_stream(
        self, request: ResponsesAgentRequest
    ) -> Generator[ResponsesAgentStreamEvent, None, None]:
        """
        Streaming prediction.
        
        Args:
            request: ResponsesAgentRequest
            
        Yields:
            ResponsesAgentStreamEvent objects
        """
        messages = to_chat_completions_input([i.model_dump() for i in request.input])
        if self.system_prompt:
            messages.insert(0, {"role": "system", "content": self.system_prompt})
        yield from self.call_and_run_tools(messages=messages)


def create_agent(
    agent_name: str,
    llm_endpoint: str,
    uc_tool_names: list[str],
    system_prompt: str
) -> RiskBricksAgent:
    """
    Factory function to create a RiskBricks agent with UC tools.
    
    Args:
        agent_name: Name for the agent
        llm_endpoint: Databricks Foundation Model endpoint
        uc_tool_names: List of Unity Catalog function names
        system_prompt: System prompt for the agent
        
    Returns:
        Configured RiskBricksAgent
    """
    # Create UC toolkit
    uc_toolkit = UCFunctionToolkit(function_names=uc_tool_names)
    uc_function_client = get_uc_function_client()
    
    # Create tool infos
    tool_infos = []
    for tool_spec in uc_toolkit.tools:
        tool_infos.append(create_tool_info(tool_spec, uc_function_client=uc_function_client))
    
    # Create and return agent
    agent = RiskBricksAgent(
        llm_endpoint=llm_endpoint,
        tools=tool_infos,
        system_prompt=system_prompt
    )
    
    return agent


# For MLflow logging
mlflow.openai.autolog()
