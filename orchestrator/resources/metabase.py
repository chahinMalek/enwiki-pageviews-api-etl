import time

import httpx
from dagster import ConfigurableResource, get_dagster_logger
from pydantic import PrivateAttr


class MetabaseResource(ConfigurableResource):
    """
    Dagster resource for programmatic Metabase management.

    Handles first-time setup, authentication and database connections.
    Contains saved questions (cards), and dashboard CRUD operations via the Metabase API.
    """

    metabase_url: str
    metabase_email: str
    metabase_password: str
    request_timeout_seconds: float = 30.0
    startup_max_wait_seconds: float = 120.0
    startup_poll_interval_seconds: float = 5.0

    _session_token: str | None = PrivateAttr(default=None)

    def _headers(self) -> dict[str, str]:
        headers: dict[str, str] = {"Content-Type": "application/json"}
        if self._session_token:
            headers["X-Metabase-Session"] = self._session_token
        return headers

    def _request(self, method: str, path: str, **kwargs) -> httpx.Response:
        """Make an authenticated request to the Metabase API."""
        with httpx.Client(
            base_url=self.metabase_url,
            headers=self._headers(),
            timeout=self.request_timeout_seconds,
        ) as client:
            response = getattr(client, method)(path, **kwargs)
            response.raise_for_status()
            return response

    def wait_until_ready(self) -> None:
        logger = get_dagster_logger()
        logger.info(f"Waiting for Metabase at {self.metabase_url}...")
        deadline = time.monotonic() + self.startup_max_wait_seconds

        while time.monotonic() < deadline:
            try:
                with httpx.Client(timeout=5.0) as client:
                    resp = client.get(f"{self.metabase_url}/api/health")
                    if resp.status_code == 200 and resp.json().get("status") == "ok":
                        logger.info("Metabase is ready")
                        return
            except httpx.HTTPError:
                pass
            time.sleep(self.startup_poll_interval_seconds)

        raise RuntimeError(f"Metabase not ready after {self.startup_max_wait_seconds}s")

    def setup(self) -> None:
        """
        Perform first-time Metabase setup if not already done.
        Creates the admin user. Idempotent — skips if already set up.
        """
        logger = get_dagster_logger()

        with httpx.Client(
            base_url=self.metabase_url, timeout=self.request_timeout_seconds
        ) as client:
            resp = client.get("/api/session/properties")
            resp.raise_for_status()
            props = resp.json()

        if props.get("has-user-setup"):
            logger.info("Metabase already set up, skipping initial setup")
            return

        setup_token = props.get("setup-token")
        if not setup_token:
            logger.warning("No setup token available, skipping initial setup")
            return

        logger.info("Performing first-time Metabase setup")
        with httpx.Client(
            base_url=self.metabase_url, timeout=self.request_timeout_seconds
        ) as client:
            resp = client.post(
                "/api/setup",
                json={
                    "token": setup_token,
                    "user": {
                        "email": self.metabase_email,
                        "password": self.metabase_password,
                        "first_name": "WikiPulse",
                        "last_name": "Admin",
                        "site_name": "WikiPulse",
                    },
                    "prefs": {
                        "site_name": "WikiPulse",
                        "site_locale": "en",
                    },
                },
            )
            resp.raise_for_status()
            # Setup response includes a session token
            data = resp.json()
            if isinstance(data, dict) and "id" in data:
                self._session_token = data["id"]
        logger.info("Metabase first-time setup complete")

    def authenticate(self) -> None:
        logger = get_dagster_logger()
        with httpx.Client(
            base_url=self.metabase_url, timeout=self.request_timeout_seconds
        ) as client:
            resp = client.post(
                "/api/session",
                json={
                    "username": self.metabase_email,
                    "password": self.metabase_password,
                },
            )
            resp.raise_for_status()
            self._session_token = resp.json()["id"]
        logger.info("Authenticated with Metabase")

    def get_databases(self) -> list[dict]:
        resp = self._request("get", "/api/database")
        return resp.json()["data"]

    def create_database(self, name: str, engine: str, details: dict) -> dict:
        resp = self._request(
            "post",
            "/api/database",
            json={"name": name, "engine": engine, "details": details},
        )
        return resp.json()

    def ensure_database(self, name: str, engine: str, details: dict) -> int:
        """
        Find or create a database connection by name.
        Returns the database ID.
        """
        logger = get_dagster_logger()
        for db in self.get_databases():
            if db["name"] == name:
                logger.info(f"Database connection '{name}' exists (id={db['id']})")
                return db["id"]

        db = self.create_database(name, engine, details)
        logger.info(f"Created database connection '{name}' (id={db['id']})")
        return db["id"]

    def get_cards(self) -> list[dict]:
        resp = self._request("get", "/api/card")
        return resp.json()

    def find_card(self, name: str) -> dict | None:
        for card in self.get_cards():
            if card["name"] == name:
                return card
        return None

    def create_card(
        self,
        name: str,
        database_id: int,
        query: str,
        display: str = "table",
        visualization_settings: dict | None = None,
        template_tags: dict | None = None,
    ) -> dict:
        """Create a saved question with a native SQL query."""
        logger = get_dagster_logger()
        native: dict = {"query": query}
        if template_tags:
            native["template-tags"] = template_tags

        resp = self._request(
            "post",
            "/api/card",
            json={
                "name": name,
                "dataset_query": {
                    "type": "native",
                    "native": native,
                    "database": database_id,
                },
                "display": display,
                "visualization_settings": visualization_settings or {},
            },
        )
        card = resp.json()
        logger.info(f"Created card '{name}' (id={card['id']})")
        return card

    def update_card(
        self,
        card_id: int,
        database_id: int,
        query: str,
        display: str = "table",
        visualization_settings: dict | None = None,
        template_tags: dict | None = None,
    ) -> dict:
        """Update an existing saved question."""
        logger = get_dagster_logger()
        native: dict = {"query": query}
        if template_tags:
            native["template-tags"] = template_tags

        resp = self._request(
            "put",
            f"/api/card/{card_id}",
            json={
                "dataset_query": {
                    "type": "native",
                    "native": native,
                    "database": database_id,
                },
                "display": display,
                "visualization_settings": visualization_settings or {},
            },
        )
        card = resp.json()
        logger.info(f"Updated card id={card_id}")
        return card

    def ensure_card(
        self,
        name: str,
        database_id: int,
        query: str,
        display: str = "table",
        visualization_settings: dict | None = None,
        template_tags: dict | None = None,
    ) -> dict:
        """Find or create/update a saved question by name. Idempotent."""
        existing = self.find_card(name)
        if existing:
            return self.update_card(
                card_id=existing["id"],
                database_id=database_id,
                query=query,
                display=display,
                visualization_settings=visualization_settings,
                template_tags=template_tags,
            )
        return self.create_card(
            name=name,
            database_id=database_id,
            query=query,
            display=display,
            visualization_settings=visualization_settings,
            template_tags=template_tags,
        )

    def get_dashboards(self) -> list[dict]:
        resp = self._request("get", "/api/dashboard")
        return resp.json()

    def find_dashboard(self, name: str) -> dict | None:
        for dash in self.get_dashboards():
            if dash["name"] == name:
                return dash
        return None

    def create_dashboard(self, name: str, description: str = "") -> dict:
        logger = get_dagster_logger()
        resp = self._request(
            "post",
            "/api/dashboard",
            json={"name": name, "description": description},
        )
        dashboard = resp.json()
        logger.info(f"Created dashboard '{name}' (id={dashboard['id']})")
        return dashboard

    def update_dashboard_cards(self, dashboard_id: int, dashcards: list[dict]) -> dict:
        """Set the card layout on a dashboard."""
        logger = get_dagster_logger()
        resp = self._request(
            "put",
            f"/api/dashboard/{dashboard_id}",
            json={"dashcards": dashcards},
        )
        logger.info(
            f"Updated dashboard id={dashboard_id} with {len(dashcards)} cards"
        )
        return resp.json()

    def ensure_dashboard(
        self,
        name: str,
        description: str = "",
        dashcards: list[dict] | None = None,
    ) -> dict:
        """Find or create a dashboard, then update its cards. Idempotent."""
        logger = get_dagster_logger()
        existing = self.find_dashboard(name)
        if existing:
            dashboard = existing
            logger.info(f"Dashboard '{name}' exists (id={dashboard['id']})")
        else:
            dashboard = self.create_dashboard(name, description)

        if dashcards is not None:
            self.update_dashboard_cards(dashboard["id"], dashcards)

        return dashboard
