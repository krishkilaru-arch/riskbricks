# Contributing to RiskBricks

Thank you for your interest in contributing to RiskBricks! This project demonstrates Agent Bricks for financial risk analytics and is designed for both educational and commercial purposes.

## Ways to Contribute

### 🐛 Bug Reports
- Use the [GitHub Issues](https://github.com/krishkilaru-arch/riskbricks/issues) to report bugs
- Include detailed steps to reproduce, expected behavior, and actual behavior
- Add screenshots or error messages when applicable

### 💡 Feature Requests
- Open an issue with the "enhancement" label
- Describe the proposed feature and its use case
- Explain how it aligns with the project's goals

### 🔧 Code Contributions
- Fork the repository
- Create a feature branch: `git checkout -b feature/amazing-feature`
- Make your changes following our coding standards
- Test your changes thoroughly
- Submit a pull request

## Development Setup

### Prerequisites
- Databricks workspace with Unity Catalog enabled
- Python 3.8+ (for local development)
- Git

### Local Development
```bash
# Clone the repository
git clone https://github.com/krishkilaru-arch/riskbricks.git
cd riskbricks

# Install dependencies (if any)
pip install -r requirements.txt

# Run data generation
python data/generate_sample_data.py

# Import notebooks into Databricks workspace
```

### Coding Standards
- Follow PEP 8 for Python code
- Use meaningful variable and function names
- Add docstrings to functions and classes
- Keep notebooks clean and well-commented

### Testing
- Test notebooks in Databricks environment
- Verify data pipelines work end-to-end
- Check that all agents function correctly

## Project Structure
```
riskbricks/
├── notebooks/          # Databricks notebooks
│   ├── ingestion/     # Data ingestion pipelines
│   ├── validation/    # DLT validation
│   ├── analytics/     # Risk computations
│   ├── agents/        # Agent Bricks workflows
│   └── dashboard/     # Demo dashboards
├── data/              # Sample datasets
├── docs/              # Documentation
├── LICENSE            # MIT License
└── README.md          # Project overview
```

## Commit Guidelines
- Use clear, descriptive commit messages
- Reference issue numbers when applicable
- Keep commits focused on single changes

## License
By contributing to this project, you agree that your contributions will be licensed under the MIT License.

## Questions?
- Open an issue for technical questions
- Check the documentation in the `docs/` folder
- Review existing issues and pull requests

Thank you for helping make RiskBricks better! 🚀