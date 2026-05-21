"""
Migration MCP server entry point. Run with: python -m migration_mcp
Set MIGRATION_SERVER_URL (default http://localhost:5001) so tools can call the Flask API.
"""
from migration_mcp.server import run

if __name__ == "__main__":
    run()
