from core.readers.dlt import Read
import dlt
from dlt.sources.helpers import requests
from typing import (
    Any)



class ReadGithub(Read):

    def __init__(self, taskname):
        super().__init__(taskname)

    def fetch_github_data(self, base_github_url, endpoint, params={}):
        """Fetch data from GitHub API based on endpoint and params."""
        url = f"{base_github_url}/{endpoint}"

        while True:
            response = requests.get(url, params=params)
            response.raise_for_status()
            yield response.json()

            # Get next page
            if "next" not in response.links:
                break
            url = response.links["next"]["url"]

    @dlt.source
    def github_source(self, base_github_url, endpoints):
        for endpoint in endpoints:
            params = {"per_page": 100}
            yield dlt.resource(
                self.fetch_github_data(base_github_url, endpoint, params),
                name=endpoint,
                write_disposition="merge",
                primary_key="id",
            )

    def enrich(self, task):
        for vals in self.readitems["issues"]:
            if "body" in vals:
                if len(vals["body"]) >= 512:
                    continue

                res = task.do(
                    text=vals["body"],
                )

                vals["nsfw_score"] = res.number()

        return self

    def do(self, base_github_url, endpoints, *args:Any, **kwargs: Any):
        source = self.github_source(base_github_url, endpoints)
        super().do(source, *args, **kwargs )

