from __future__ import annotations

import json
import urllib.error
import urllib.request
from pathlib import Path

from app.core.config import Settings


LANGUAGE_LABELS = {
    "afr": "Afrikáans",
    "amh": "Amhárico",
    "ara": "Árabe",
    "asm": "Asamés",
    "aze": "Azerí",
    "aze_cyrl": "Azerí cirílico",
    "bel": "Bielorruso",
    "ben": "Bengalí",
    "bod": "Tibetano",
    "bos": "Bosnio",
    "bre": "Bretón",
    "bul": "Búlgaro",
    "cat": "Catalán",
    "ceb": "Cebuano",
    "ces": "Checo",
    "chi_sim": "Chino simplificado",
    "chi_sim_vert": "Chino simplificado vertical",
    "chi_tra": "Chino tradicional",
    "chi_tra_vert": "Chino tradicional vertical",
    "chr": "Cherokee",
    "cos": "Corso",
    "cym": "Galés",
    "dan": "Danés",
    "dan_frak": "Danés fraktur",
    "deu": "Alemán",
    "deu_frak": "Alemán fraktur",
    "deu_latf": "Alemán latino fraktur",
    "div": "Maldivo",
    "dzo": "Dzongkha",
    "ell": "Griego",
    "eng": "Inglés",
    "enm": "Inglés medio",
    "epo": "Esperanto",
    "equ": "Ecuaciones",
    "est": "Estonio",
    "eus": "Euskera",
    "fao": "Feroés",
    "fas": "Persa",
    "fil": "Filipino",
    "fin": "Finlandés",
    "fra": "Francés",
    "frm": "Francés medio",
    "fry": "Frisón",
    "gla": "Gaélico escocés",
    "gle": "Irlandés",
    "glg": "Gallego",
    "grc": "Griego antiguo",
    "guj": "Guyaratí",
    "hat": "Criollo haitiano",
    "heb": "Hebreo",
    "hin": "Hindi",
    "hrv": "Croata",
    "hun": "Húngaro",
    "hye": "Armenio",
    "iku": "Inuktitut",
    "ind": "Indonesio",
    "isl": "Islandés",
    "ita": "Italiano",
    "ita_old": "Italiano antiguo",
    "jav": "Javanés",
    "jpn": "Japonés",
    "jpn_vert": "Japonés vertical",
    "kan": "Canarés",
    "kat": "Georgiano",
    "kat_old": "Georgiano antiguo",
    "kaz": "Kazajo",
    "khm": "Jemer",
    "kir": "Kirguís",
    "kmr": "Kurdo kurmanji",
    "kor": "Coreano",
    "kor_vert": "Coreano vertical",
    "lao": "Lao",
    "lat": "Latín",
    "lav": "Letón",
    "lit": "Lituano",
    "ltz": "Luxemburgués",
    "mal": "Malayalam",
    "mar": "Maratí",
    "mkd": "Macedonio",
    "mlt": "Maltés",
    "mon": "Mongol",
    "mri": "Maorí",
    "msa": "Malayo",
    "mya": "Birmano",
    "nep": "Nepalí",
    "nld": "Neerlandés",
    "nor": "Noruego",
    "oci": "Occitano",
    "ori": "Odia",
    "osd": "Orientación y script",
    "pan": "Panyabí",
    "pol": "Polaco",
    "por": "Portugués",
    "pus": "Pastún",
    "que": "Quechua",
    "ron": "Rumano",
    "rus": "Ruso",
    "san": "Sánscrito",
    "sin": "Cingalés",
    "slk": "Eslovaco",
    "slk_frak": "Eslovaco fraktur",
    "slv": "Esloveno",
    "snd": "Sindhi",
    "spa": "Español",
    "spa_old": "Español antiguo",
    "sqi": "Albanés",
    "srp": "Serbio",
    "srp_latn": "Serbio latino",
    "sun": "Sundanés",
    "swa": "Suajili",
    "swe": "Sueco",
    "syr": "Siríaco",
    "tam": "Tamil",
    "tat": "Tártaro",
    "tel": "Telugu",
    "tgk": "Tayiko",
    "tgl": "Tagalo",
    "tha": "Tailandés",
    "tir": "Tigriña",
    "ton": "Tongano",
    "tur": "Turco",
    "uig": "Uigur",
    "ukr": "Ucraniano",
    "urd": "Urdu",
    "uzb": "Uzbeko",
    "uzb_cyrl": "Uzbeko cirílico",
    "vie": "Vietnamita",
    "yid": "Yidis",
    "yor": "Yoruba",
}


class LanguageService:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.tessdata_dir = Path(settings.tesseract_tessdata_dir)

    def ensure_dir(self) -> None:
        self.tessdata_dir.mkdir(parents=True, exist_ok=True)

    def _label(self, code: str) -> str:
        return LANGUAGE_LABELS.get(code, code.replace("_", " ").strip().title())

    def list_installed_codes(self) -> list[str]:
        self.ensure_dir()
        return sorted(
            p.stem for p in self.tessdata_dir.glob("*.traineddata") if p.is_file()
        )

    def list_installed(self) -> list[dict]:
        installed = set(self.list_installed_codes())
        result = []
        for code in sorted(installed):
            if code == "osd":
                continue
            result.append({
                "code": code,
                "label": self._label(code),
                "installed": True,
            })
        return result

    def fetch_catalog(self) -> list[dict]:
        req = urllib.request.Request(
            self.settings.tesseract_catalog_api,
            headers={"Accept": "application/vnd.github+json", "User-Agent": "queai-ocr"},
        )

        try:
            with urllib.request.urlopen(req, timeout=20) as resp:
                payload = json.loads(resp.read().decode("utf-8"))
        except Exception:
            payload = []

        installed = set(self.list_installed_codes())
        catalog = []

        for item in payload:
            name = item.get("name", "")
            if not name.endswith(".traineddata"):
                continue

            code = name[:-12]
            if code == "osd":
                continue

            catalog.append({
                "code": code,
                "label": self._label(code),
                "installed": code in installed,
            })

        catalog.sort(key=lambda x: (not x["installed"], x["label"].lower()))
        return catalog

    def install_language(self, code: str) -> dict:
        self.ensure_dir()

        code = (code or "").strip()
        if not code:
            raise ValueError("Código de idioma vacío")

        if code == "all_installed":
            raise ValueError("Esa opción no se descarga")

        dest = self.tessdata_dir / f"{code}.traineddata"
        if dest.exists():
            return {
                "code": code,
                "label": self._label(code),
                "installed": True,
                "already_installed": True,
            }

        url = f"{self.settings.tesseract_catalog_raw_base}/{code}.traineddata"
        tmp = self.tessdata_dir / f"{code}.traineddata.tmp"

        try:
            req = urllib.request.Request(url, headers={"User-Agent": "queai-ocr"})
            with urllib.request.urlopen(req, timeout=60) as resp:
                data = resp.read()
        except urllib.error.HTTPError as exc:
            raise ValueError(f"No se pudo descargar el idioma '{self._label(code)}' (HTTP {exc.code})") from exc
        except Exception as exc:
            raise ValueError(f"No se pudo descargar el idioma '{self._label(code)}': {exc}") from exc

        tmp.write_bytes(data)
        tmp.replace(dest)

        return {
            "code": code,
            "label": self._label(code),
            "installed": True,
            "already_installed": False,
        }

    def install_languages(self, codes: list[str]) -> dict:
        installed = []
        errors = []

        for code in codes:
            try:
                installed.append(self.install_language(code))
            except Exception as exc:
                errors.append({"code": code, "error": str(exc)})

        return {
            "installed": installed,
            "errors": errors,
            "current_installed": self.list_installed(),
        }

    def processing_options(self) -> list[dict]:
        installed = self.list_installed()
        options = []

        for item in installed:
            options.append({
                "value": item["code"],
                "label": item["label"],
                "installed": True,
                "downloadable": False,
            })

        if len(installed) > 1:
            options.insert(0, {
                "value": "all_installed",
                "label": "Todos los idiomas instalados",
                "installed": True,
                "downloadable": False,
            })

        return options

    def normalize_processing_selection(self, value: str) -> str:
        installed = self.list_installed_codes()
        installed = [code for code in installed if code != "osd"]

        value = (value or "").strip()
        if not value:
            return self.settings.tesseract_default_lang

        if value == "all_installed":
            if not installed:
                return self.settings.tesseract_default_lang
            return "+".join(installed)

        if value not in installed:
            raise ValueError(f"El idioma seleccionado no está instalado: {value}")

        return value