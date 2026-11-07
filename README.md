# Multi-Vendor FMCG ETL & Automated Reconciliation Engine

An end-to-end Python data engineering pipeline designed to consolidate decentralized distributor ERP reports, synchronize internal BI APIs, audit daily file compliance, and export clean reconciliation datasets to Google Sheets.

## Business Problem & Impact
In multi-tier corporate distribution without centralized manufacturer ERP access, fragmented daily reports across dozens of distribution hubs create inventory blind spots, delayed sales posting, and reconciliation bottlenecks.
* **Inventory Discrepancy Reduction:** Cut stock mismatches by **70%** and eliminated **2.5 Cr BDT** in overstock through automated system-to-system reconciliation.
* **Working Capital Recovery:** Reduced unrecorded month-end Telco inventory by **82%**, unlocking **~65 Lac BDT** in working capital within 60 days.
* **Throughput:** Processes **5,000+ Excel/XLS files** per batch using multi-threaded I/O and Rust-backed Excel parsing (`python-calamine`).

## Architecture Overview
1. **Multi-Threaded Vendor ETL (`pipelines/vendor_*_etl.py`)**
   * Uses `ThreadPoolExecutor` and `python-calamine` for high-speed parallel ingestion across nested directories.
   * Implements dynamic header row auto-detection, whitespace sanitization, multi-format date parsing, and SKU-level price enrichment.
2. **Concurrent BI REST API Sync (`pipelines/bi_api_sync.py`)**
   * Automates session authentication and concurrent CSV query extraction across 9+ operational endpoints (Sales, Payments, GRN vs. Payment, Closing Stock, Outward Picklists).
   * Splits high-volume queries into dynamic 10-day date chunks to prevent API timeouts.
3. **Automated Compliance Auditor (`pipelines/compliance_audit.py`)**
   * Performs a cross-join between master distributor/report matrices and daily uploaded files to flag missing submissions automatically.
4. **Resilient Google Sheets Exporter (`utils/sheets_exporter.py`)**
   * Dynamically resizes target worksheets to prevent cell-limit errors and implements exponential backoff retries for HTTP 429 rate limits.

## Quick Start
1. Clone the repository and install dependencies:
   ```bash
   pip install -r requirements.txt