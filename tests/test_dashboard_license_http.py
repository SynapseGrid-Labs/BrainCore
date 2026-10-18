"""Exercise license discovery and serving through the real app without a database."""

import os
import subprocess
import sys
from pathlib import Path


def test_dashboard_license_link_serves_notice_instead_of_spa(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[1]
    distribution = tmp_path / "dashboard" / "dist"
    assets = distribution / "assets"
    assets.mkdir(parents=True)
    # Keep the actual entry page and HTTP routes. Only the built license payload
    # is synthetic; the browser gate verifies full licenses in the actual build.
    (distribution / "index.html").write_text((root / "dashboard" / "index.html").read_text())
    (assets / "THIRD_PARTY_LICENSES.md").write_text("# Licenses\nSynthetic dependency notice\n")
    probe = """
import asyncio
from html.parser import HTMLParser
import httpx
from braincore_memory.api import app

class Links(HTMLParser):
    license_url = None
    def handle_starttag(self, tag, attrs):
        attributes = dict(attrs)
        if tag == 'link' and attributes.get('rel') == 'license':
            self.license_url = attributes.get('href')

async def main():
    # ASGITransport deliberately does not enter the database-opening lifespan.
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url='http://test'
    ) as client:
        index = await client.get('/')
        assert index.status_code == 200
        links = Links()
        links.feed(index.text)
        assert links.license_url == '/assets/THIRD_PARTY_LICENSES.md'
        license = await client.get(links.license_url)
        assert license.status_code == 200
        assert 'text/html' not in license.headers['content-type']
        assert license.text == '# Licenses\\nSynthetic dependency notice\\n'
        missing_asset = await client.get('/assets/missing-license.md')
        assert missing_asset.status_code == 404
        spa = await client.get('/memories')
        assert spa.text == index.text
        missing_api = await client.get('/v1/missing-resource')
        assert missing_api.status_code == 404
        assert missing_api.json()['error']['code'] == 'resource_not_found'

asyncio.run(main())
"""
    result = subprocess.run(  # noqa: S603 - interpreter and inline probe are trusted test code
        [sys.executable, "-c", probe],
        cwd=tmp_path,
        env={**os.environ, "PYTHONPATH": str(root / "src")},
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
