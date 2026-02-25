from __future__ import annotations

from typing import Any

import httpx
import structlog
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from src.models import (
    APIFetchError,
    InstitutionRecord,
    ReportCatalogEntry,
    ReportValue,
)
from src.settings import Settings

logger = structlog.get_logger()


class IFDataClient:
    """Synchronous client for the IF.data Olinda/OData API."""

    def __init__(self, settings: Settings) -> None:
        self._base_url = settings.ifdata_base_url.rstrip("/")
        self._timeout = settings.ifdata_timeout_sec
        self._max_retries = settings.ifdata_max_retries
        self._client = httpx.Client(
            timeout=httpx.Timeout(self._timeout),
            follow_redirects=True,
        )

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> IFDataClient:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()

    @retry(  # type: ignore[untyped-decorator]
        retry=retry_if_exception_type(httpx.TransportError),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=30),
    )
    def _get_json(
        self,
        endpoint: str,
        params: dict[str, str | int] | None = None,
    ) -> list[dict[str, Any]]:
        """Fetch paginated OData endpoint, returning all records."""
        all_records: list[dict[str, Any]] = []
        url: str | None = f"{self._base_url}/{endpoint}"
        query_params: dict[str, str] = {"$format": "json", "$top": "1000"}
        if params:
            for k, v in params.items():
                query_params[k] = str(v)

        while url is not None:
            response = self._client.get(url, params=query_params)
            if response.status_code != 200:
                raise APIFetchError(endpoint, response.status_code, response.text[:500])
            data = response.json()
            records: list[dict[str, Any]] = data.get("value", [])
            all_records.extend(records)

            # OData pagination
            url = data.get("@odata.nextLink") or data.get("odata.nextLink")
            query_params = {}  # nextLink already contains params

            logger.debug(
                "ifdata_page_fetched", endpoint=endpoint, records=len(records)
            )

        return all_records

    def fetch_catalog(self) -> list[ReportCatalogEntry]:
        """Fetch report catalog from ListaDeRelatorio."""
        raw = self._get_json("ListaDeRelatorio")
        return [ReportCatalogEntry.model_validate(r) for r in raw]

    def fetch_cadastro(self, ano_mes: int) -> list[InstitutionRecord]:
        """Fetch institution registry for a given quarter."""
        raw = self._get_json(
            "IfDataCadastro(AnoMes=@AnoMes)",
            params={"@AnoMes": ano_mes},
        )
        return [InstitutionRecord.model_validate(r) for r in raw]

    def fetch_report_values(
        self, ano_mes: int, tipo_instituicao: int, relatorio: str
    ) -> list[ReportValue]:
        """Fetch all values for a specific report/period/institution type."""
        raw = self._get_json(
            "IfDataValores(AnoMes=@AnoMes,TipoInstituicao=@TipoInstituicao,"
            "Relatorio=@Relatorio)",
            params={
                "@AnoMes": ano_mes,
                "@TipoInstituicao": tipo_instituicao,
                "@Relatorio": f"'{relatorio}'",
            },
        )
        return [ReportValue.model_validate(r) for r in raw]
