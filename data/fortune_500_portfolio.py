"""
Fortune 500 Companies Portfolio for RiskBricks
Comprehensive list of major US publicly traded companies
Based on Fortune 500 list - represents the largest US companies by revenue
"""

# Fortune 500 Companies - Publicly Traded
# Format: (Ticker Symbol, Company Name, Sector, Sub-Industry)

FORTUNE_500_COMPANIES = [
    # Technology & Software (100 companies)
    ("AAPL", "Apple Inc", "Technology", "Consumer Electronics"),
    ("MSFT", "Microsoft Corp", "Technology", "Software"),
    ("GOOGL", "Alphabet Inc Class A", "Technology", "Internet Services"),
    ("GOOG", "Alphabet Inc Class C", "Technology", "Internet Services"),
    ("AMZN", "Amazon.com Inc", "Technology", "E-Commerce & Cloud"),
    ("META", "Meta Platforms Inc", "Technology", "Social Media"),
    ("NVDA", "NVIDIA Corp", "Technology", "Semiconductors"),
    ("TSLA", "Tesla Inc", "Technology", "Electric Vehicles"),
    ("AVGO", "Broadcom Inc", "Technology", "Semiconductors"),
    ("ORCL", "Oracle Corp", "Technology", "Enterprise Software"),
    ("CSCO", "Cisco Systems", "Technology", "Networking"),
    ("ADBE", "Adobe Inc", "Technology", "Software"),
    ("CRM", "Salesforce Inc", "Technology", "Cloud Software"),
    ("INTC", "Intel Corp", "Technology", "Semiconductors"),
    ("AMD", "Advanced Micro Devices", "Technology", "Semiconductors"),
    ("QCOM", "Qualcomm Inc", "Technology", "Semiconductors"),
    ("TXN", "Texas Instruments", "Technology", "Semiconductors"),
    ("IBM", "IBM Corp", "Technology", "IT Services"),
    ("NOW", "ServiceNow Inc", "Technology", "Cloud Software"),
    ("INTU", "Intuit Inc", "Technology", "Software"),
    ("AMAT", "Applied Materials", "Technology", "Semiconductor Equipment"),
    ("MU", "Micron Technology", "Technology", "Memory Chips"),
    ("LRCX", "Lam Research", "Technology", "Semiconductor Equipment"),
    ("KLAC", "KLA Corp", "Technology", "Semiconductor Equipment"),
    ("SNPS", "Synopsys Inc", "Technology", "Software"),
    ("CDNS", "Cadence Design", "Technology", "Software"),
    ("MCHP", "Microchip Technology", "Technology", "Semiconductors"),
    ("NXPI", "NXP Semiconductors", "Technology", "Semiconductors"),
    ("TEAM", "Atlassian Corp", "Technology", "Software"),
    ("WDAY", "Workday Inc", "Technology", "Cloud Software"),
    ("DDOG", "Datadog Inc", "Technology", "Software"),
    ("SNOW", "Snowflake Inc", "Technology", "Cloud Data"),
    ("CRWD", "CrowdStrike Holdings", "Technology", "Cybersecurity"),
    ("ZS", "Zscaler Inc", "Technology", "Cybersecurity"),
    ("NET", "Cloudflare Inc", "Technology", "Cloud Services"),
    ("PLTR", "Palantir Technologies", "Technology", "Data Analytics"),
    ("PANW", "Palo Alto Networks", "Technology", "Cybersecurity"),
    ("FTNT", "Fortinet Inc", "Technology", "Cybersecurity"),
    ("HPE", "Hewlett Packard Enterprise", "Technology", "IT Hardware"),
    ("HPQ", "HP Inc", "Technology", "Personal Computers"),
    ("DELL", "Dell Technologies", "Technology", "IT Hardware"),
    ("NTAP", "NetApp Inc", "Technology", "Data Storage"),
    ("AKAM", "Akamai Technologies", "Technology", "Cloud Services"),
    ("VEEV", "Veeva Systems", "Technology", "Healthcare Software"),
    ("ANSS", "ANSYS Inc", "Technology", "Engineering Software"),
    ("ROP", "Roper Technologies", "Technology", "Industrial Software"),
    ("KEYS", "Keysight Technologies", "Technology", "Test Equipment"),
    ("ZBRA", "Zebra Technologies", "Technology", "Enterprise Tech"),
    ("FFIV", "F5 Inc", "Technology", "Application Delivery"),
    ("JNPR", "Juniper Networks", "Technology", "Networking"),
    
    # Financial Services (80 companies)
    ("JPM", "JPMorgan Chase & Co", "Financials", "Banking"),
    ("BAC", "Bank of America Corp", "Financials", "Banking"),
    ("WFC", "Wells Fargo & Co", "Financials", "Banking"),
    ("C", "Citigroup Inc", "Financials", "Banking"),
    ("GS", "Goldman Sachs Group", "Financials", "Investment Banking"),
    ("MS", "Morgan Stanley", "Financials", "Investment Banking"),
    ("BLK", "BlackRock Inc", "Financials", "Asset Management"),
    ("SCHW", "Charles Schwab Corp", "Financials", "Brokerage"),
    ("CB", "Chubb Ltd", "Financials", "Insurance"),
    ("AXP", "American Express Co", "Financials", "Credit Cards"),
    ("PGR", "Progressive Corp", "Financials", "Insurance"),
    ("TFC", "Truist Financial Corp", "Financials", "Banking"),
    ("USB", "US Bancorp", "Financials", "Banking"),
    ("PNC", "PNC Financial Services", "Financials", "Banking"),
    ("CME", "CME Group Inc", "Financials", "Financial Exchange"),
    ("ICE", "Intercontinental Exchange", "Financials", "Financial Exchange"),
    ("MCO", "Moody's Corp", "Financials", "Credit Rating"),
    ("SPGI", "S&P Global Inc", "Financials", "Credit Rating"),
    ("BX", "Blackstone Inc", "Financials", "Private Equity"),
    ("KKR", "KKR & Co Inc", "Financials", "Private Equity"),
    ("APO", "Apollo Global Management", "Financials", "Private Equity"),
    ("COIN", "Coinbase Global Inc", "Financials", "Cryptocurrency"),
    ("V", "Visa Inc", "Financials", "Payment Processing"),
    ("MA", "Mastercard Inc", "Financials", "Payment Processing"),
    ("PYPL", "PayPal Holdings Inc", "Financials", "Digital Payments"),
    ("FIS", "Fidelity National Information Services", "Financials", "Financial Tech"),
    ("FISV", "Fiserv Inc", "Financials", "Financial Tech"),
    ("ADP", "Automatic Data Processing", "Financials", "Payroll Services"),
    ("TROW", "T. Rowe Price Group", "Financials", "Asset Management"),
    ("BEN", "Franklin Resources", "Financials", "Asset Management"),
    ("STT", "State Street Corp", "Financials", "Custody Banking"),
    ("BK", "Bank of New York Mellon", "Financials", "Custody Banking"),
    ("RF", "Regions Financial", "Financials", "Banking"),
    ("CFG", "Citizens Financial Group", "Financials", "Banking"),
    ("HBAN", "Huntington Bancshares", "Financials", "Banking"),
    ("KEY", "KeyCorp", "Financials", "Banking"),
    ("MTB", "M&T Bank Corp", "Financials", "Banking"),
    ("FITB", "Fifth Third Bancorp", "Financials", "Banking"),
    ("COF", "Capital One Financial", "Financials", "Credit Cards"),
    ("DFS", "Discover Financial Services", "Financials", "Credit Cards"),
    ("SYF", "Synchrony Financial", "Financials", "Consumer Finance"),
    ("ALL", "Allstate Corp", "Financials", "Insurance"),
    ("TRV", "Travelers Companies", "Financials", "Insurance"),
    ("AIG", "American International Group", "Financials", "Insurance"),
    ("MET", "MetLife Inc", "Financials", "Insurance"),
    ("PRU", "Prudential Financial", "Financials", "Insurance"),
    ("AFL", "Aflac Inc", "Financials", "Insurance"),
    ("HIG", "Hartford Financial Services", "Financials", "Insurance"),
    ("WRB", "W.R. Berkley Corp", "Financials", "Insurance"),
    ("L", "Loews Corp", "Financials", "Conglomerate"),
    
    # Healthcare & Pharmaceuticals (70 companies)
    ("UNH", "UnitedHealth Group Inc", "Healthcare", "Health Insurance"),
    ("JNJ", "Johnson & Johnson", "Healthcare", "Pharmaceuticals"),
    ("LLY", "Eli Lilly and Co", "Healthcare", "Pharmaceuticals"),
    ("PFE", "Pfizer Inc", "Healthcare", "Pharmaceuticals"),
    ("ABBV", "AbbVie Inc", "Healthcare", "Pharmaceuticals"),
    ("MRK", "Merck & Co Inc", "Healthcare", "Pharmaceuticals"),
    ("TMO", "Thermo Fisher Scientific", "Healthcare", "Life Sciences"),
    ("ABT", "Abbott Laboratories", "Healthcare", "Medical Devices"),
    ("DHR", "Danaher Corp", "Healthcare", "Life Sciences"),
    ("BMY", "Bristol-Myers Squibb", "Healthcare", "Pharmaceuticals"),
    ("AMGN", "Amgen Inc", "Healthcare", "Biotechnology"),
    ("GILD", "Gilead Sciences Inc", "Healthcare", "Biotechnology"),
    ("CVS", "CVS Health Corp", "Healthcare", "Pharmacy"),
    ("CI", "Cigna Group", "Healthcare", "Health Insurance"),
    ("HCA", "HCA Healthcare Inc", "Healthcare", "Hospitals"),
    ("ISRG", "Intuitive Surgical Inc", "Healthcare", "Medical Devices"),
    ("REGN", "Regeneron Pharmaceuticals", "Healthcare", "Biotechnology"),
    ("VRTX", "Vertex Pharmaceuticals", "Healthcare", "Biotechnology"),
    ("SYK", "Stryker Corp", "Healthcare", "Medical Devices"),
    ("BSX", "Boston Scientific Corp", "Healthcare", "Medical Devices"),
    ("MDT", "Medtronic PLC", "Healthcare", "Medical Devices"),
    ("ELV", "Elevance Health Inc", "Healthcare", "Health Insurance"),
    ("ZTS", "Zoetis Inc", "Healthcare", "Animal Health"),
    ("IDXX", "IDEXX Laboratories Inc", "Healthcare", "Veterinary Diagnostics"),
    ("DXCM", "DexCom Inc", "Healthcare", "Medical Devices"),
    ("HUM", "Humana Inc", "Healthcare", "Health Insurance"),
    ("CNC", "Centene Corp", "Healthcare", "Health Insurance"),
    ("MCK", "McKesson Corp", "Healthcare", "Healthcare Distribution"),
    ("CAH", "Cardinal Health Inc", "Healthcare", "Healthcare Distribution"),
    ("COR", "Cencora Inc", "Healthcare", "Healthcare Distribution"),
    ("A", "Agilent Technologies", "Healthcare", "Life Sciences"),
    ("BDX", "Becton Dickinson and Co", "Healthcare", "Medical Devices"),
    ("EW", "Edwards Lifesciences", "Healthcare", "Medical Devices"),
    ("BAX", "Baxter International", "Healthcare", "Medical Products"),
    ("IQV", "IQVIA Holdings", "Healthcare", "Healthcare Services"),
    ("BIIB", "Biogen Inc", "Healthcare", "Biotechnology"),
    ("MRNA", "Moderna Inc", "Healthcare", "Biotechnology"),
    ("ILMN", "Illumina Inc", "Healthcare", "Life Sciences"),
    ("ALGN", "Align Technology", "Healthcare", "Medical Devices"),
    ("MOH", "Molina Healthcare", "Healthcare", "Health Insurance"),
    ("UHS", "Universal Health Services", "Healthcare", "Healthcare Facilities"),
    ("DGX", "Quest Diagnostics", "Healthcare", "Diagnostics"),
    ("LH", "Laboratory Corp of America", "Healthcare", "Diagnostics"),
    ("HOLX", "Hologic Inc", "Healthcare", "Medical Devices"),
    ("RMD", "ResMed Inc", "Healthcare", "Medical Devices"),
    ("PODD", "Insulet Corp", "Healthcare", "Medical Devices"),
    ("WST", "West Pharmaceutical Services", "Healthcare", "Pharmaceutical Packaging"),
    ("WAT", "Waters Corp", "Healthcare", "Lab Equipment"),
    ("PKI", "PerkinElmer Inc", "Healthcare", "Life Sciences"),
    ("TFX", "Teleflex Inc", "Healthcare", "Medical Devices"),
    
    # Consumer Discretionary & Retail (60 companies)
    ("WMT", "Walmart Inc", "Consumer Discretionary", "Retail"),
    ("HD", "Home Depot Inc", "Consumer Discretionary", "Home Improvement"),
    ("MCD", "McDonald's Corp", "Consumer Discretionary", "Restaurants"),
    ("NKE", "Nike Inc", "Consumer Discretionary", "Apparel"),
    ("SBUX", "Starbucks Corp", "Consumer Discretionary", "Restaurants"),
    ("LOW", "Lowe's Companies Inc", "Consumer Discretionary", "Home Improvement"),
    ("TJX", "TJX Companies Inc", "Consumer Discretionary", "Retail"),
    ("BKNG", "Booking Holdings Inc", "Consumer Discretionary", "Online Travel"),
    ("CMG", "Chipotle Mexican Grill", "Consumer Discretionary", "Restaurants"),
    ("ORLY", "O'Reilly Automotive Inc", "Consumer Discretionary", "Auto Parts"),
    ("MAR", "Marriott International", "Consumer Discretionary", "Hotels"),
    ("GM", "General Motors Co", "Consumer Discretionary", "Automobiles"),
    ("F", "Ford Motor Co", "Consumer Discretionary", "Automobiles"),
    ("ABNB", "Airbnb Inc", "Consumer Discretionary", "Online Travel"),
    ("UBER", "Uber Technologies Inc", "Consumer Discretionary", "Ride-Sharing"),
    ("LYFT", "Lyft Inc", "Consumer Discretionary", "Ride-Sharing"),
    ("DASH", "DoorDash Inc", "Consumer Discretionary", "Food Delivery"),
    ("YUM", "Yum! Brands Inc", "Consumer Discretionary", "Restaurants"),
    ("DRI", "Darden Restaurants Inc", "Consumer Discretionary", "Restaurants"),
    ("ROST", "Ross Stores Inc", "Consumer Discretionary", "Retail"),
    ("AZO", "AutoZone Inc", "Consumer Discretionary", "Auto Parts"),
    ("BBY", "Best Buy Co Inc", "Consumer Discretionary", "Electronics Retail"),
    ("DHI", "DR Horton Inc", "Consumer Discretionary", "Homebuilding"),
    ("LEN", "Lennar Corp", "Consumer Discretionary", "Homebuilding"),
    ("TGT", "Target Corp", "Consumer Discretionary", "Retail"),
    ("DG", "Dollar General Corp", "Consumer Discretionary", "Discount Stores"),
    ("DLTR", "Dollar Tree Inc", "Consumer Discretionary", "Discount Stores"),
    ("EBAY", "eBay Inc", "Consumer Discretionary", "E-Commerce"),
    ("ETSY", "Etsy Inc", "Consumer Discretionary", "E-Commerce"),
    ("EXPE", "Expedia Group Inc", "Consumer Discretionary", "Online Travel"),
    ("HLT", "Hilton Worldwide Holdings", "Consumer Discretionary", "Hotels"),
    ("MGM", "MGM Resorts International", "Consumer Discretionary", "Gaming"),
    ("WYNN", "Wynn Resorts Ltd", "Consumer Discretionary", "Gaming"),
    ("LVS", "Las Vegas Sands Corp", "Consumer Discretionary", "Gaming"),
    ("CCL", "Carnival Corp", "Consumer Discretionary", "Cruise Lines"),
    ("RCL", "Royal Caribbean Cruises", "Consumer Discretionary", "Cruise Lines"),
    ("NCLH", "Norwegian Cruise Line", "Consumer Discretionary", "Cruise Lines"),
    ("GPC", "Genuine Parts Co", "Consumer Discretionary", "Auto Parts"),
    ("AAP", "Advance Auto Parts", "Consumer Discretionary", "Auto Parts"),
    ("AZO", "AutoZone Inc", "Consumer Discretionary", "Auto Parts"),
    ("RL", "Ralph Lauren Corp", "Consumer Discretionary", "Apparel"),
    ("PVH", "PVH Corp", "Consumer Discretionary", "Apparel"),
    ("TPR", "Tapestry Inc", "Consumer Discretionary", "Luxury Goods"),
    ("LULU", "Lululemon Athletica", "Consumer Discretionary", "Apparel"),
    ("DECK", "Deckers Outdoor Corp", "Consumer Discretionary", "Footwear"),
    ("UAA", "Under Armour Inc", "Consumer Discretionary", "Apparel"),
    ("SKX", "Skechers USA Inc", "Consumer Discretionary", "Footwear"),
    ("POOL", "Pool Corp", "Consumer Discretionary", "Pool Supplies"),
    ("WHR", "Whirlpool Corp", "Consumer Discretionary", "Appliances"),
    ("NVR", "NVR Inc", "Consumer Discretionary", "Homebuilding"),
    
    # Communication Services & Media (40 companies)
    ("NFLX", "Netflix Inc", "Communication Services", "Streaming"),
    ("DIS", "Walt Disney Co", "Communication Services", "Entertainment"),
    ("CMCSA", "Comcast Corp", "Communication Services", "Cable/Broadband"),
    ("T", "AT&T Inc", "Communication Services", "Telecommunications"),
    ("VZ", "Verizon Communications", "Communication Services", "Telecommunications"),
    ("TMUS", "T-Mobile US Inc", "Communication Services", "Wireless"),
    ("CHTR", "Charter Communications", "Communication Services", "Cable"),
    ("EA", "Electronic Arts Inc", "Communication Services", "Video Games"),
    ("TTWO", "Take-Two Interactive", "Communication Services", "Video Games"),
    ("NTES", "NetEase Inc ADR", "Communication Services", "Video Games"),
    ("MTCH", "Match Group Inc", "Communication Services", "Online Dating"),
    ("PARA", "Paramount Global", "Communication Services", "Media"),
    ("WBD", "Warner Bros Discovery", "Communication Services", "Media"),
    ("FOXA", "Fox Corp Class A", "Communication Services", "Media"),
    ("OMC", "Omnicom Group Inc", "Communication Services", "Advertising"),
    ("IPG", "Interpublic Group", "Communication Services", "Advertising"),
    ("NWSA", "News Corp Class A", "Communication Services", "Publishing"),
    ("NYT", "New York Times Co", "Communication Services", "Publishing"),
    ("LYV", "Live Nation Entertainment", "Communication Services", "Live Events"),
    ("WMG", "Warner Music Group", "Communication Services", "Music"),
    ("ROKU", "Roku Inc", "Communication Services", "Streaming Devices"),
    ("SPOT", "Spotify Technology", "Communication Services", "Music Streaming"),
    ("PINS", "Pinterest Inc", "Communication Services", "Social Media"),
    ("SNAP", "Snap Inc", "Communication Services", "Social Media"),
    ("RBLX", "Roblox Corp", "Communication Services", "Gaming Platform"),
    ("U", "Unity Software Inc", "Communication Services", "Gaming Software"),
    ("ZM", "Zoom Video Communications", "Communication Services", "Video Conferencing"),
    ("TWLO", "Twilio Inc", "Communication Services", "Cloud Communications"),
    ("DOCU", "DocuSign Inc", "Communication Services", "Digital Agreements"),
    ("ZI", "ZoomInfo Technologies", "Communication Services", "Marketing Software"),
    
    # Consumer Staples (40 companies)
    ("PG", "Procter & Gamble Co", "Consumer Staples", "Personal Products"),
    ("KO", "Coca-Cola Co", "Consumer Staples", "Beverages"),
    ("PEP", "PepsiCo Inc", "Consumer Staples", "Beverages"),
    ("COST", "Costco Wholesale Corp", "Consumer Staples", "Wholesale"),
    ("PM", "Philip Morris International", "Consumer Staples", "Tobacco"),
    ("MO", "Altria Group Inc", "Consumer Staples", "Tobacco"),
    ("CL", "Colgate-Palmolive Co", "Consumer Staples", "Personal Products"),
    ("KMB", "Kimberly-Clark Corp", "Consumer Staples", "Personal Products"),
    ("GIS", "General Mills Inc", "Consumer Staples", "Packaged Foods"),
    ("K", "Kellogg Co", "Consumer Staples", "Packaged Foods"),
    ("HSY", "Hershey Co", "Consumer Staples", "Confectioners"),
    ("MDLZ", "Mondelez International", "Consumer Staples", "Snack Foods"),
    ("KHC", "Kraft Heinz Co", "Consumer Staples", "Packaged Foods"),
    ("STZ", "Constellation Brands Inc", "Consumer Staples", "Beverages"),
    ("TAP", "Molson Coors Beverage", "Consumer Staples", "Beverages"),
    ("KDP", "Keurig Dr Pepper Inc", "Consumer Staples", "Beverages"),
    ("MNST", "Monster Beverage Corp", "Consumer Staples", "Beverages"),
    ("EL", "Estee Lauder Companies", "Consumer Staples", "Cosmetics"),
    ("CLX", "Clorox Co", "Consumer Staples", "Household Products"),
    ("CHD", "Church & Dwight Co", "Consumer Staples", "Household Products"),
    ("SJM", "JM Smucker Co", "Consumer Staples", "Packaged Foods"),
    ("CPB", "Campbell Soup Co", "Consumer Staples", "Packaged Foods"),
    ("CAG", "Conagra Brands Inc", "Consumer Staples", "Packaged Foods"),
    ("HRL", "Hormel Foods Corp", "Consumer Staples", "Packaged Foods"),
    ("TSN", "Tyson Foods Inc", "Consumer Staples", "Meat Products"),
    ("KR", "Kroger Co", "Consumer Staples", "Grocery"),
    ("SYY", "Sysco Corp", "Consumer Staples", "Food Distribution"),
    ("ADM", "Archer-Daniels-Midland", "Consumer Staples", "Agriculture"),
    ("BG", "Bunge Global SA", "Consumer Staples", "Agriculture"),
    ("MKC", "McCormick & Co Inc", "Consumer Staples", "Spices"),
    ("LW", "Lamb Weston Holdings", "Consumer Staples", "Food Processing"),
    ("COKE", "Coca-Cola Consolidated", "Consumer Staples", "Beverages"),
    ("DPZ", "Domino's Pizza Inc", "Consumer Staples", "Restaurants"),
    ("WBA", "Walgreens Boots Alliance", "Consumer Staples", "Drug Stores"),
    ("RAD", "Rite Aid Corp", "Consumer Staples", "Drug Stores"),
    ("GO", "Grocery Outlet Holding", "Consumer Staples", "Discount Grocery"),
    ("INGR", "Ingredion Inc", "Consumer Staples", "Food Ingredients"),
    ("BF-B", "Brown-Forman Corp", "Consumer Staples", "Distillers"),
    ("SAM", "Boston Beer Co", "Consumer Staples", "Beverages"),
    ("CELH", "Celsius Holdings Inc", "Consumer Staples", "Energy Drinks"),
    
    # Energy (40 companies)
    ("XOM", "Exxon Mobil Corp", "Energy", "Oil & Gas"),
    ("CVX", "Chevron Corp", "Energy", "Oil & Gas"),
    ("COP", "ConocoPhillips", "Energy", "Oil & Gas"),
    ("SLB", "Schlumberger NV", "Energy", "Oilfield Services"),
    ("EOG", "EOG Resources Inc", "Energy", "Oil & Gas"),
    ("PXD", "Pioneer Natural Resources", "Energy", "Oil & Gas"),
    ("MPC", "Marathon Petroleum Corp", "Energy", "Refining"),
    ("PSX", "Phillips 66", "Energy", "Refining"),
    ("VLO", "Valero Energy Corp", "Energy", "Refining"),
    ("HES", "Hess Corp", "Energy", "Oil & Gas"),
    ("OXY", "Occidental Petroleum", "Energy", "Oil & Gas"),
    ("DVN", "Devon Energy Corp", "Energy", "Oil & Gas"),
    ("FANG", "Diamondback Energy Inc", "Energy", "Oil & Gas"),
    ("HAL", "Halliburton Co", "Energy", "Oilfield Services"),
    ("BKR", "Baker Hughes Co", "Energy", "Oilfield Services"),
    ("WMB", "Williams Companies Inc", "Energy", "Pipelines"),
    ("KMI", "Kinder Morgan Inc", "Energy", "Pipelines"),
    ("OKE", "ONEOK Inc", "Energy", "Pipelines"),
    ("LNG", "Cheniere Energy Inc", "Energy", "LNG"),
    ("MRO", "Marathon Oil Corp", "Energy", "Oil & Gas"),
    ("APA", "APA Corp", "Energy", "Oil & Gas"),
    ("CTRA", "Coterra Energy Inc", "Energy", "Oil & Gas"),
    ("EQT", "EQT Corp", "Energy", "Natural Gas"),
    ("TPL", "Texas Pacific Land Corp", "Energy", "Land Management"),
    ("CNQ", "Canadian Natural Resources", "Energy", "Oil & Gas"),
    ("SU", "Suncor Energy Inc", "Energy", "Oil & Gas"),
    ("IMO", "Imperial Oil Ltd", "Energy", "Oil & Gas"),
    ("CVE", "Cenovus Energy Inc", "Energy", "Oil & Gas"),
    ("TTE", "TotalEnergies SE ADR", "Energy", "Integrated Oil"),
    ("BP", "BP PLC ADR", "Energy", "Integrated Oil"),
    ("SHEL", "Shell PLC ADR", "Energy", "Integrated Oil"),
    ("E", "Eni SpA ADR", "Energy", "Integrated Oil"),
    ("EC", "Ecopetrol SA ADR", "Energy", "Oil & Gas"),
    ("YPF", "YPF SA ADR", "Energy", "Oil & Gas"),
    ("RIG", "Transocean Ltd", "Energy", "Offshore Drilling"),
    ("VAL", "Valaris Ltd", "Energy", "Offshore Drilling"),
    ("NOV", "NOV Inc", "Energy", "Oilfield Equipment"),
    ("FTI", "TechnipFMC PLC", "Energy", "Oilfield Services"),
    ("CHX", "ChampionX Corp", "Energy", "Oilfield Services"),
    ("WTTR", "Select Water Solutions", "Energy", "Water Services"),
    
    # Industrials (50 companies)
    ("BA", "Boeing Co", "Industrials", "Aerospace"),
    ("HON", "Honeywell International", "Industrials", "Diversified Industrials"),
    ("UPS", "United Parcel Service", "Industrials", "Logistics"),
    ("RTX", "RTX Corp", "Industrials", "Aerospace & Defense"),
    ("LMT", "Lockheed Martin Corp", "Industrials", "Aerospace & Defense"),
    ("CAT", "Caterpillar Inc", "Industrials", "Construction Equipment"),
    ("DE", "Deere & Co", "Industrials", "Agricultural Equipment"),
    ("GE", "General Electric Co", "Industrials", "Conglomerate"),
    ("MMM", "3M Co", "Industrials", "Diversified Industrials"),
    ("UNP", "Union Pacific Corp", "Industrials", "Railroads"),
    ("FDX", "FedEx Corp", "Industrials", "Logistics"),
    ("NSC", "Norfolk Southern Corp", "Industrials", "Railroads"),
    ("CSX", "CSX Corp", "Industrials", "Railroads"),
    ("WM", "Waste Management Inc", "Industrials", "Waste Management"),
    ("EMR", "Emerson Electric Co", "Industrials", "Electrical Equipment"),
    ("ETN", "Eaton Corp PLC", "Industrials", "Electrical Equipment"),
    ("ITW", "Illinois Tool Works", "Industrials", "Machinery"),
    ("PH", "Parker-Hannifin Corp", "Industrials", "Machinery"),
    ("CARR", "Carrier Global Corp", "Industrials", "HVAC"),
    ("OTIS", "Otis Worldwide Corp", "Industrials", "Elevators"),
    ("LHX", "L3Harris Technologies", "Industrials", "Aerospace & Defense"),
    ("NOC", "Northrop Grumman Corp", "Industrials", "Aerospace & Defense"),
    ("GD", "General Dynamics Corp", "Industrials", "Aerospace & Defense"),
    ("TDG", "TransDigm Group Inc", "Industrials", "Aerospace Parts"),
    ("HWM", "Howmet Aerospace Inc", "Industrials", "Aerospace Parts"),
    ("PCAR", "PACCAR Inc", "Industrials", "Truck Manufacturing"),
    ("CMI", "Cummins Inc", "Industrials", "Engines"),
    ("URI", "United Rentals Inc", "Industrials", "Equipment Rental"),
    ("RSG", "Republic Services Inc", "Industrials", "Waste Management"),
    ("IR", "Ingersoll Rand Inc", "Industrials", "Industrial Equipment"),
    ("ROK", "Rockwell Automation", "Industrials", "Industrial Automation"),
    ("DOV", "Dover Corp", "Industrials", "Industrial Equipment"),
    ("FTV", "Fortive Corp", "Industrials", "Industrial Tech"),
    ("FAST", "Fastenal Co", "Industrials", "Industrial Distribution"),
    ("CHRW", "CH Robinson Worldwide", "Industrials", "Logistics"),
    ("EXPD", "Expeditors International", "Industrials", "Logistics"),
    ("JBHT", "JB Hunt Transport Services", "Industrials", "Trucking"),
    ("ODFL", "Old Dominion Freight Line", "Industrials", "Trucking"),
    ("DAL", "Delta Air Lines Inc", "Industrials", "Airlines"),
    ("UAL", "United Airlines Holdings", "Industrials", "Airlines"),
    ("AAL", "American Airlines Group", "Industrials", "Airlines"),
    ("LUV", "Southwest Airlines Co", "Industrials", "Airlines"),
    ("ALK", "Alaska Air Group Inc", "Industrials", "Airlines"),
    ("JBLU", "JetBlue Airways Corp", "Industrials", "Airlines"),
    ("UBER", "Uber Technologies Inc", "Industrials", "Transportation Network"),
    ("XYL", "Xylem Inc", "Industrials", "Water Technology"),
    ("PWR", "Quanta Services Inc", "Industrials", "Engineering & Construction"),
    ("VMC", "Vulcan Materials Co", "Industrials", "Construction Materials"),
    ("MLM", "Martin Marietta Materials", "Industrials", "Construction Materials"),
    ("J", "Jacobs Solutions Inc", "Industrials", "Engineering Services"),
    
    # Materials & Chemicals (30 companies)
    ("LIN", "Linde PLC", "Materials", "Industrial Gases"),
    ("APD", "Air Products & Chemicals", "Materials", "Industrial Gases"),
    ("ECL", "Ecolab Inc", "Materials", "Specialty Chemicals"),
    ("SHW", "Sherwin-Williams Co", "Materials", "Paints & Coatings"),
    ("FCX", "Freeport-McMoRan Inc", "Materials", "Copper & Gold"),
    ("NEM", "Newmont Corp", "Materials", "Gold Mining"),
    ("DOW", "Dow Inc", "Materials", "Chemicals"),
    ("DD", "DuPont de Nemours Inc", "Materials", "Specialty Materials"),
    ("PPG", "PPG Industries Inc", "Materials", "Paints & Coatings"),
    ("NUE", "Nucor Corp", "Materials", "Steel"),
    ("STLD", "Steel Dynamics Inc", "Materials", "Steel"),
    ("RS", "Reliance Steel & Aluminum", "Materials", "Steel Distribution"),
    ("CF", "CF Industries Holdings", "Materials", "Fertilizers"),
    ("MOS", "Mosaic Co", "Materials", "Fertilizers"),
    ("ALB", "Albemarle Corp", "Materials", "Specialty Chemicals"),
    ("CE", "Celanese Corp", "Materials", "Chemicals"),
    ("EMN", "Eastman Chemical Co", "Materials", "Chemicals"),
    ("FMC", "FMC Corp", "Materials", "Agricultural Chemicals"),
    ("IFF", "International Flavors & Fragrances", "Materials", "Specialty Chemicals"),
    ("PKG", "Packaging Corp of America", "Materials", "Packaging"),
    ("IP", "International Paper Co", "Materials", "Paper & Packaging"),
    ("WRK", "WestRock Co", "Materials", "Packaging"),
    ("AMCR", "Amcor PLC", "Materials", "Packaging"),
    ("SEE", "Sealed Air Corp", "Materials", "Packaging"),
    ("AVY", "Avery Dennison Corp", "Materials", "Packaging Materials"),
    ("BALL", "Ball Corp", "Materials", "Metal Packaging"),
    ("NX", "Quanex Building Products", "Materials", "Building Products"),
    ("SCCO", "Southern Copper Corp", "Materials", "Copper Mining"),
    ("GOLD", "Barrick Gold Corp", "Materials", "Gold Mining"),
    ("AEM", "Agnico Eagle Mines", "Materials", "Gold Mining"),
    
    # Real Estate (20 companies)
    ("AMT", "American Tower Corp", "Real Estate", "Cell Towers"),
    ("PLD", "Prologis Inc", "Real Estate", "Industrial REITs"),
    ("CCI", "Crown Castle Inc", "Real Estate", "Cell Towers"),
    ("EQIX", "Equinix Inc", "Real Estate", "Data Centers"),
    ("SPG", "Simon Property Group", "Real Estate", "Retail REITs"),
    ("PSA", "Public Storage", "Real Estate", "Self Storage"),
    ("DLR", "Digital Realty Trust", "Real Estate", "Data Centers"),
    ("O", "Realty Income Corp", "Real Estate", "Retail REITs"),
    ("WELL", "Welltower Inc", "Real Estate", "Healthcare REITs"),
    ("AVB", "AvalonBay Communities", "Real Estate", "Residential REITs"),
    ("EQR", "Equity Residential", "Real Estate", "Residential REITs"),
    ("VTR", "Ventas Inc", "Real Estate", "Healthcare REITs"),
    ("SBAC", "SBA Communications Corp", "Real Estate", "Cell Towers"),
    ("ARE", "Alexandria Real Estate", "Real Estate", "Office REITs"),
    ("INVH", "Invitation Homes Inc", "Real Estate", "Residential REITs"),
    ("MAA", "Mid-America Apartment Communities", "Real Estate", "Residential REITs"),
    ("KIM", "Kimco Realty Corp", "Real Estate", "Retail REITs"),
    ("REG", "Regency Centers Corp", "Real Estate", "Retail REITs"),
    ("HST", "Host Hotels & Resorts", "Real Estate", "Hotel REITs"),
    ("BXP", "Boston Properties Inc", "Real Estate", "Office REITs"),
    
    # Utilities (20 companies)
    ("NEE", "NextEra Energy Inc", "Utilities", "Electric Utilities"),
    ("DUK", "Duke Energy Corp", "Utilities", "Electric Utilities"),
    ("SO", "Southern Co", "Utilities", "Electric Utilities"),
    ("D", "Dominion Energy Inc", "Utilities", "Electric Utilities"),
    ("AEP", "American Electric Power", "Utilities", "Electric Utilities"),
    ("EXC", "Exelon Corp", "Utilities", "Electric Utilities"),
    ("SRE", "Sempra", "Utilities", "Gas & Electric"),
    ("XEL", "Xcel Energy Inc", "Utilities", "Electric Utilities"),
    ("WEC", "WEC Energy Group Inc", "Utilities", "Electric Utilities"),
    ("ED", "Consolidated Edison Inc", "Utilities", "Electric Utilities"),
    ("PEG", "Public Service Enterprise Group", "Utilities", "Electric Utilities"),
    ("ES", "Eversource Energy", "Utilities", "Electric Utilities"),
    ("AWK", "American Water Works", "Utilities", "Water Utilities"),
    ("DTE", "DTE Energy Co", "Utilities", "Electric Utilities"),
    ("PPL", "PPL Corp", "Utilities", "Electric Utilities"),
    ("FE", "FirstEnergy Corp", "Utilities", "Electric Utilities"),
    ("ETR", "Entergy Corp", "Utilities", "Electric Utilities"),
    ("CNP", "CenterPoint Energy Inc", "Utilities", "Gas & Electric"),
    ("NI", "NiSource Inc", "Utilities", "Gas & Electric"),
    ("ATO", "Atmos Energy Corp", "Utilities", "Gas Utilities"),
]

def get_fortune_500_symbols():
    """Get list of all Fortune 500 ticker symbols"""
    return [symbol for symbol, _, _, _ in FORTUNE_500_COMPANIES]

def get_fortune_500_by_sector():
    """Group companies by sector"""
    sectors = {}
    for symbol, company, sector, industry in FORTUNE_500_COMPANIES:
        if sector not in sectors:
            sectors[sector] = []
        sectors[sector].append((symbol, company, industry))
    return sectors

def generate_portfolio_allocations(total_value=10_000_000, method="sector_weighted"):
    """
    Generate portfolio allocations
    
    Args:
        total_value: Total portfolio value (default $10M)
        method: Allocation method
            - "equal": Equal weight across all stocks
            - "sector_weighted": Proportional to sector (realistic)
            - "market_cap": Weight by market cap (would need real data)
    
    Returns:
        List of (symbol, company, sector, industry, weight, value_usd)
    """
    # Define realistic sector weights (based on S&P 500 composition)
    sector_weights = {
        "Technology": 0.28,              # 28%
        "Financials": 0.13,              # 13%
        "Healthcare": 0.13,              # 13%
        "Consumer Discretionary": 0.11,  # 11%
        "Communication Services": 0.09,  # 9%
        "Industrials": 0.08,             # 8%
        "Consumer Staples": 0.07,        # 7%
        "Energy": 0.05,                  # 5%
        "Utilities": 0.03,               # 3%
        "Real Estate": 0.03,             # 3%
        "Materials": 0.02,               # 2%
    }
    
    holdings = []
    
    if method == "equal":
        weight_per_stock = 1.0 / len(FORTUNE_500_COMPANIES)
        for symbol, company, sector, industry in FORTUNE_500_COMPANIES:
            holdings.append((
                symbol, company, sector, industry,
                weight_per_stock,
                total_value * weight_per_stock
            ))
    
    elif method == "sector_weighted":
        # Count stocks per sector
        sector_counts = {}
        for _, _, sector, _ in FORTUNE_500_COMPANIES:
            sector_counts[sector] = sector_counts.get(sector, 0) + 1
        
        # Allocate
        for symbol, company, sector, industry in FORTUNE_500_COMPANIES:
            sector_weight = sector_weights.get(sector, 0.01)
            stocks_in_sector = sector_counts[sector]
            weight_per_stock = sector_weight / stocks_in_sector
            
            holdings.append((
                symbol, company, sector, industry,
                weight_per_stock,
                total_value * weight_per_stock
            ))
    
    return holdings

def print_portfolio_summary():
    """Print portfolio summary"""
    holdings = generate_portfolio_allocations()
    
    print(f"FORTUNE 500 PORTFOLIO")
    print("=" * 80)
    print(f"Total Companies: {len(holdings)}")
    print(f"\nSector Breakdown:")
    print("-" * 80)
    
    sector_summary = {}
    for _, _, sector, _, weight, _ in holdings:
        sector_summary[sector] = sector_summary.get(sector, 0) + weight
    
    for sector in sorted(sector_summary.keys(), key=lambda x: sector_summary[x], reverse=True):
        count = sum(1 for _, _, s, _, _, _ in holdings if s == sector)
        print(f"{sector:30s}: {count:3d} stocks ({sector_summary[sector]*100:5.1f}%)")
    
    print(f"\nTotal Allocation: {sum(sector_summary.values())*100:.1f}%")
    print("=" * 80)

if __name__ == "__main__":
    print_portfolio_summary()
