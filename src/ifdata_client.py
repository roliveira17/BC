from __future__ import annotations

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
    DataNotFoundError,
    InstitutionRecord,
    ReportValue,
)
from src.settings import Settings

logger = structlog.get_logger()

REPORT_NAME_MAP: dict[str, str] = {
    "1": "Resumo",
    "4": "Demonstra\u00e7\u00e3o de Resultado",
    "5": "Informa\u00e7\u00f5es de Capital",
}

_METADATA_LID_THRESHOLD = 100


class IFDataClient:
    """Synchronous client for the IF.data REST API (www3.bcb.gov.br)."""

    def __init__(self, settings: Settings) -> None:
        self._base_url = settings.ifdata_base_url.rstrip("/")
        self._tipo_inst = settings.ifdata_tipo_inst_id
        self._timeout = settings.ifdata_timeout_sec
        self._max_retries = settings.ifdata_max_retries
        self._client = httpx.Client(
            timeout=httpx.Timeout(self._timeout),
            follow_redirects=True,
            headers={"User-Agent": "BC-Dashboard/1.0"},
        )
        self._period_index: dict[int, list[dict[str, object]]] | None = None

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
    def _get_json(self, endpoint: str) -> object:
        """Fetch a single JSON endpoint."""
        url = f"{self._base_url}/{endpoint}"
        response = self._client.get(url)
        if response.status_code != 200:
            raise APIFetchError(endpoint, response.status_code, response.text[:500])
        return response.json()

    @retry(  # type: ignore[untyped-decorator]
        retry=retry_if_exception_type(httpx.TransportError),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=30),
    )
    def _fetch_file(self, file_path: str) -> object:
        """Fetch a JSON file via the arquivos endpoint."""
        url = f"{self._base_url}/arquivos"
        response = self._client.get(url, params={"nomeArquivo": file_path})
        if response.status_code != 200:
            raise APIFetchError(
                f"arquivos/{file_path}", response.status_code, response.text[:500]
            )
        return response.json()

    def _ensure_index(self) -> dict[int, list[dict[str, object]]]:
        """Fetch and cache the period index from both endpoints."""
        if self._period_index is not None:
            return self._period_index

        index: dict[int, list[dict[str, object]]] = {}
        for endpoint in ["relatorios2000a2024", "relatorios2025a2030"]:
            data = self._get_json(endpoint)
            if not isinstance(data, list):
                continue
            for entry in data:
                dt = entry["dt"]
                index[dt] = entry["files"]
            logger.info("ifdata_index_loaded", endpoint=endpoint, periods=len(data))

        self._period_index = index
        return index

    def list_periods(self) -> list[int]:
        """Return all available period codes, most recent first."""
        index = self._ensure_index()
        return sorted(index.keys(), reverse=True)

    def _find_file(self, ano_mes: int, suffix: str) -> str | None:
        """Find a file path in the index matching the given suffix."""
        index = self._ensure_index()
        files = index.get(ano_mes, [])
        for f in files:
            if (
                isinstance(f, dict)
                and isinstance(f.get("f"), str)
                and f["f"].endswith(suffix)
            ):
                return f["f"]
        return None

    def _find_trel_id(self, ano_mes: int, report_name: str) -> int | None:
        """Find the trel ID matching report name + tipo_inst for a period."""
        index = self._ensure_index()
        files = index.get(ano_mes, [])
        for f in files:
            trel = f.get("trel") if isinstance(f, dict) else None
            if not trel or not isinstance(trel, dict):
                continue
            name = trel.get("n", "")
            if not isinstance(name, str) or not name.strip().startswith(report_name):
                continue
            sels = trel.get("s", [])
            if any(s.get("id") == self._tipo_inst for s in sels):
                return trel["id"]
        return None

    def fetch_cadastro(self, ano_mes: int) -> list[InstitutionRecord]:
        """Fetch institution registry for a given period."""
        suffix = f"cadastro{ano_mes}_{self._tipo_inst}.json"
        file_path = self._find_file(ano_mes, suffix)
        if not file_path:
            raise DataNotFoundError(ano_mes)

        raw = self._fetch_file(file_path)
        if not isinstance(raw, list):
            raise DataNotFoundError(ano_mes)

        records: list[InstitutionRecord] = []
        for entry in raw:
            records.append(
                InstitutionRecord(
                    cod_conglomerado=int(entry["c0"]),
                    nome_conglomerado=entry["c2"],
                    cod_inst=int(entry["c0"]),
                    nome_inst=entry["c2"],
                    cnpj=entry["c0"],
                    segmento=entry.get("c12", ""),
                    tipo_instituicao=self._tipo_inst,
                    cidade=entry.get("c11", ""),
                    uf=entry.get("c10", ""),
                )
            )
        logger.info("cadastro_fetched", ano_mes=ano_mes, records=len(records))
        return records

    def fetch_report_values(
        self, ano_mes: int, tipo_inst: int, relatorio: str
    ) -> list[ReportValue]:
        """Fetch report values for a specific period/report/institution type."""
        report_name = REPORT_NAME_MAP.get(relatorio)
        if not report_name:
            logger.warning("unknown_report_code", relatorio=relatorio)
            return []

        trel_id = self._find_trel_id(ano_mes, report_name)
        if trel_id is None:
            logger.warning(
                "trel_not_found",
                ano_mes=ano_mes,
                report=report_name,
                tipo_inst=self._tipo_inst,
            )
            return []

        # Fetch trel (report layout)
        trel_suffix = f"trel{ano_mes}_{trel_id}.json"
        trel_path = self._find_file(ano_mes, trel_suffix)
        if not trel_path:
            return []
        trel = self._fetch_file(trel_path)
        if not isinstance(trel, dict):
            return []

        # Fetch info (indicator metadata)
        info_path = self._find_file(ano_mes, f"info{ano_mes}.json")
        if not info_path:
            return []
        info_list = self._fetch_file(info_path)
        if not isinstance(info_list, list):
            return []
        info_map = {i["id"]: i for i in info_list}

        # Build column definitions from trel
        columns = _extract_columns(trel, info_map)
        if not columns:
            return []

        dados_groups = {col[2] for col in columns}

        # Fetch dados files for needed groups
        entity_values = _load_dados(
            self, ano_mes, dados_groups
        )

        # Fetch cadastro for entity filtering and names
        cadastro_suffix = f"cadastro{ano_mes}_{tipo_inst}.json"
        cadastro_path = self._find_file(ano_mes, cadastro_suffix)
        entity_names: dict[int, str] = {}
        valid_entities: set[int] = set()
        if cadastro_path:
            cadastro_raw = self._fetch_file(cadastro_path)
            if isinstance(cadastro_raw, list):
                for c in cadastro_raw:
                    eid = int(c["c0"])
                    entity_names[eid] = c["c2"]
                    valid_entities.add(eid)

        # Build ReportValue records
        result: list[ReportValue] = []
        for eid in sorted(entity_values.keys()):
            if valid_entities and eid not in valid_entities:
                continue
            ename = entity_names.get(eid, str(eid))
            for name, lid, _group, order in columns:
                value = entity_values[eid].get(lid)
                result.append(
                    ReportValue(
                        cod_conglomerado=eid,
                        nome_conglomerado=ename,
                        codigo_coluna=str(lid),
                        nome_coluna=name,
                        valor_a=float(value) if value is not None else None,
                        nome_linha=name,
                        ordenacao=order,
                    )
                )

        logger.info(
            "report_fetched",
            ano_mes=ano_mes,
            relatorio=relatorio,
            trel_id=trel_id,
            entities=len(entity_values),
            rows=len(result),
        )
        return result


def _extract_columns(
    trel: dict[str, object],
    info_map: dict[int, dict[str, object]],
) -> list[tuple[str, int, int, int]]:
    """Extract data columns from trel layout.

    Returns list of (display_name, lid, dados_group, order).
    Skips metadata columns (low lid) and composite columns (lid=-1).
    """
    columns: list[tuple[str, int, int, int]] = []
    trel_cols = trel.get("c", [])
    if not isinstance(trel_cols, list):
        return columns

    for col in trel_cols:
        ifd = col.get("ifd")
        order = col.get("o", 0)
        inf = info_map.get(ifd)
        if not inf:
            continue
        lid = inf.get("lid", -1)
        if not isinstance(lid, int) or lid < _METADATA_LID_THRESHOLD:
            continue
        raw_name = inf.get("n", "")
        name = raw_name.split("\n")[0].strip() if isinstance(raw_name, str) else ""
        if not name:
            continue
        group = inf.get("a", 1)
        columns.append((name, lid, group, order))
    return columns


def _load_dados(
    client: IFDataClient,
    ano_mes: int,
    groups: set[int],
) -> dict[int, dict[int, float | None]]:
    """Load entity values from dados files for the specified groups.

    Returns {entity_code: {lid: value}}, excluding aggregate entity (e=0).
    """
    entity_values: dict[int, dict[int, float | None]] = {}
    for group in sorted(groups):
        dados_path = client._find_file(ano_mes, f"dados{ano_mes}_{group}.json")
        if not dados_path:
            logger.warning("dados_file_missing", ano_mes=ano_mes, group=group)
            continue
        dados = client._fetch_file(dados_path)
        if not isinstance(dados, dict):
            continue
        values_list = dados.get("values", [])
        if not isinstance(values_list, list):
            continue
        for entity in values_list:
            eid = entity.get("e")
            if eid == 0:
                continue
            if eid not in entity_values:
                entity_values[eid] = {}
            for v in entity.get("v", []):
                entity_values[eid][v["i"]] = v["v"]
    return entity_values
