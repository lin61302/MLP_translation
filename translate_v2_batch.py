#!/usr/bin/env python3
"""
Translation v2 pipeline (safe, benchmark-gated, traceable).

Key features:
- Writes to new columns only (no overwrite of existing translated fields).
- Benchmarks baseline vs candidate models before translation where public test sets exist.
- Uses sentence-preserving tokenizer-aware truncation aimed at >= target English length.
- Adds per-document translation metadata + append-only history entries.
- Emits detailed logs: run log, benchmark jsonl, progress jsonl, issue jsonl.
"""

import argparse
import gc
import json
import logging
import math
import os
import re
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional, Sequence, Tuple

# Avoid optional TensorFlow import paths that crash with legacy numpy/tf combinations.
os.environ.setdefault("TRANSFORMERS_NO_TF", "1")
os.environ.setdefault("USE_TF", "0")
os.environ.setdefault("USE_TORCH", "1")
os.environ.setdefault("TOKENIZERS_PARALLELISM", "false")

import torch
from pymongo import MongoClient, UpdateOne
from pymongo.errors import PyMongoError
from sacrebleu.metrics import BLEU, CHRF
from transformers import AutoModelForSeq2SeqLM, AutoTokenizer


DEFAULT_MONGO_URI = "mongodb://zungru:balsas.rial.tanoaks.schmoe.coffing@db-wibbels.sas.upenn.edu/?authSource=ml4p&tls=true"
DEFAULT_DB = "ml4p"
DEFAULT_LANG_ORDER = ["fr", "ar", "ru", "uk", "zh", "tr", "km2", "sr", "mk", "es2"]

INTERNATIONAL_LANGS = {"es2", "ar", "uk", "ru", "fr", "zh"}
REGIONAL_LANGS = {"fr", "es2", "ar", "sr", "mk", "ru"}

NLLB_MODEL = "facebook/nllb-200-distilled-600M"
MODEL_CANDIDATES: Dict[str, List[str]] = {
    "es2": [NLLB_MODEL],
    "fr": [NLLB_MODEL],
    "ar": [NLLB_MODEL],
    "uk": [NLLB_MODEL],
    "ru": ["facebook/wmt19-ru-en", NLLB_MODEL],
    "zh": [NLLB_MODEL],
    "tr": [NLLB_MODEL],
    "km2": [NLLB_MODEL],
    "km": [NLLB_MODEL],
    "sr": [NLLB_MODEL],
    "mk": [NLLB_MODEL],
}

NLLB_LANG_CODES = {
    "es2": "spa_Latn",
    "es": "spa_Latn",
    "fr": "fra_Latn",
    "ar": "arb_Arab",
    "uk": "ukr_Cyrl",
    "ru": "rus_Cyrl",
    "zh": "zho_Hans",
    "tr": "tur_Latn",
    "km2": "khm_Khmr",
    "km": "khm_Khmr",
    "sr": "srp_Cyrl",
    "mk": "mkd_Cyrl",
}

# Public benchmarks available by language pair for objective A/B checks.
# Format: language -> [(testset, langpair), ...]
BENCHMARK_SPECS: Dict[str, List[Tuple[str, str]]] = {
    "es2": [("wmt13", "es-en")],
    "fr": [("wmt14", "fr-en"), ("iwslt17", "fr-en")],
    "ar": [("iwslt17", "ar-en")],
    "ru": [("wmt19", "ru-en")],
    "uk": [("wmt22", "uk-en")],
    "zh": [("wmt21", "zh-en"), ("iwslt17", "zh-en")],
    "tr": [("wmt18", "tr-en"), ("wmt16", "tr-en")],
    "km2": [("wmt20", "km-en")],
    "km": [("wmt20", "km-en")],
}

# Approximate English char expansion factors (English chars / source chars).
# We compute source target chars as ceil(target_en_chars / expansion_factor).
EXPANSION_FACTOR = {
    "zh": 2.20,
    "ar": 1.40,
    "ru": 1.25,
    "uk": 1.25,
    "tr": 1.12,
    "km2": 1.70,
    "km": 1.70,
    "sr": 1.25,
    "mk": 1.25,
    "fr": 1.08,
    "es2": 1.08,
    "es": 1.08,
}

MAX_SOURCE_TOKENS_BY_LANG = {
    "zh": 280,
    "ar": 340,
    "ru": 360,
    "uk": 360,
    "tr": 360,
    "km2": 320,
    "km": 320,
    "sr": 360,
    "mk": 360,
    "fr": 380,
    "es2": 380,
    "es": 380,
}

SENT_SPLIT_RE = re.compile(r"(?<=[\.\!\?\u061f\u3002\uff01\uff1f\u0964])\s*")
SPACE_RE = re.compile(r"\s+")


def utc_ts() -> str:
    return datetime.utcnow().strftime("%Y%m%d_%H%M%S")


def utc_iso() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def write_jsonl(path: str, obj: dict) -> None:
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False) + "\n")


def setup_logger(log_path: str) -> logging.Logger:
    logger = logging.getLogger("translate_v2")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

    fh = logging.FileHandler(log_path)
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    logger.addHandler(sh)

    return logger


def iter_months(start_year: int, start_month: int, end_year: int, end_month: int):
    y, m = start_year, start_month
    while (y < end_year) or (y == end_year and m <= end_month):
        yield y, m
        m += 1
        if m > 12:
            y += 1
            m = 1


def clean_text(text: Optional[str]) -> str:
    if not isinstance(text, str):
        return ""
    text = text.replace("\n", " ").replace("\r", " ")
    text = text.replace("\u00a0", " ")
    text = SPACE_RE.sub(" ", text).strip()
    return text


def split_sentences(text: str) -> List[str]:
    text = clean_text(text)
    if not text:
        return []
    parts = [p.strip() for p in SENT_SPLIT_RE.split(text) if p and p.strip()]
    return parts if parts else [text]


def normalize_doc_language(lang: str) -> str:
    if lang == "es2":
        return "es"
    if lang == "km2":
        return "km"
    return lang


def doc_language_values(lang: str) -> List[str]:
    if lang == "es2":
        return ["es"]
    if lang in {"km2", "km"}:
        return ["km2", "km"]
    return [lang]


def target_source_chars(lang: str, target_english_chars: int) -> int:
    factor = EXPANSION_FACTOR.get(lang, 1.20)
    return int(math.ceil(float(target_english_chars) / factor))


def quality_flags(
    src_text: str,
    out_text: Optional[str],
    *,
    field: str,
    min_abs_chars: int,
    short_ratio: float,
    check_latin_ratio: bool,
) -> Tuple[List[str], dict]:
    """
    Field-aware quality checks used for problem detection and traceability.

    `field` is emitted as a prefix in every flag so we can separate title/main issues.
    """
    flags: List[str] = []
    src = clean_text(src_text)
    out = clean_text(out_text)

    metrics = {
        "source_chars": len(src),
        "output_chars": len(out),
        "short_threshold": 0,
        "alpha_chars": 0,
        "latin_chars": 0,
    }

    if not out:
        return [f"{field}_empty_output"], metrics

    if out == src:
        flags.append(f"{field}_unchanged_output")

    short_threshold = max(int(min_abs_chars), int(len(src) * float(short_ratio)))
    metrics["short_threshold"] = short_threshold
    if len(out) < short_threshold:
        flags.append(f"{field}_very_short_output")

    letters = sum(ch.isalpha() for ch in out)
    latin = sum(("a" <= ch.lower() <= "z") for ch in out)
    metrics["alpha_chars"] = letters
    metrics["latin_chars"] = latin

    if check_latin_ratio and letters >= 20 and latin / max(letters, 1) < 0.45:
        flags.append(f"{field}_low_latin_ratio")

    return flags, metrics


@dataclass
class PreparedMainText:
    text: str
    meta: dict


class TranslationModel:
    def __init__(self, model_name: str, lang: str, device: torch.device, logger: logging.Logger):
        self.model_name = model_name
        self.lang = lang
        self.device = device
        self.logger = logger
        self.kind = "nllb" if "nllb-200" in model_name else "generic"

        self.logger.info("Loading model: %s", model_name)
        self.tokenizer = AutoTokenizer.from_pretrained(model_name)
        self.model = AutoModelForSeq2SeqLM.from_pretrained(model_name)

        if self.device.type == "cuda":
            try:
                self.model = self.model.half()
            except Exception as err:
                self.logger.warning("Could not cast model to fp16 (%s): %s", model_name, err)
            self.model = self.model.to(self.device)

        self.model.eval()

        if self.kind == "nllb":
            self.nllb_src = NLLB_LANG_CODES.get(lang)
            if not self.nllb_src:
                raise ValueError(f"No NLLB source code mapping for language={lang}")
        else:
            self.nllb_src = None

    @torch.inference_mode()
    def translate_texts(
        self,
        texts: Sequence[str],
        max_input_tokens: int,
        max_new_tokens: int,
        batch_size: int,
        num_beams: int = 4,
    ) -> List[str]:
        results: List[str] = []
        if not texts:
            return results

        idx = 0
        cur_batch_size = max(1, batch_size)

        while idx < len(texts):
            chunk = list(texts[idx : idx + cur_batch_size])
            try:
                if self.kind == "nllb":
                    self.tokenizer.src_lang = self.nllb_src

                enc = self.tokenizer(
                    chunk,
                    return_tensors="pt",
                    padding=True,
                    truncation=True,
                    max_length=max_input_tokens,
                )

                if self.device.type == "cuda":
                    enc = {k: v.to(self.device) for k, v in enc.items()}

                gen_kwargs = {
                    "max_new_tokens": max_new_tokens,
                    "num_beams": num_beams,
                    "early_stopping": True,
                }
                if self.kind == "nllb":
                    gen_kwargs["forced_bos_token_id"] = self.tokenizer.lang_code_to_id["eng_Latn"]

                gen = self.model.generate(**enc, **gen_kwargs)
                out = self.tokenizer.batch_decode(
                    gen,
                    skip_special_tokens=True,
                    clean_up_tokenization_spaces=True,
                )
                results.extend([clean_text(x) for x in out])
                idx += len(chunk)

            except RuntimeError as err:
                msg = str(err).lower()
                if "out of memory" in msg and self.device.type == "cuda" and cur_batch_size > 1:
                    torch.cuda.empty_cache()
                    cur_batch_size = max(1, cur_batch_size // 2)
                    self.logger.warning("CUDA OOM on %s; reducing batch size to %d", self.model_name, cur_batch_size)
                    continue
                raise

        return results

    def prepare_maintext(self, text: str, lang: str, target_english_chars: int) -> PreparedMainText:
        raw = clean_text(text)
        if not raw:
            return PreparedMainText(
                text=".",
                meta={
                    "source_chars": 0,
                    "selected_chars": 1,
                    "source_tokens": 0,
                    "selected_tokens": 1,
                    "total_sentences": 0,
                    "kept_sentences": 1,
                    "truncated_by_tokens": False,
                    "target_source_chars": target_source_chars(lang, target_english_chars),
                },
            )

        sentences = split_sentences(raw)
        max_source_tokens = MAX_SOURCE_TOKENS_BY_LANG.get(lang, 360)
        min_chars = target_source_chars(lang, target_english_chars)

        kept: List[str] = []
        selected = ""
        selected_tokens = 0
        truncated_by_tokens = False

        for sent in sentences:
            candidate = (selected + " " + sent).strip() if selected else sent
            candidate_ids = self.tokenizer(candidate, add_special_tokens=False).input_ids
            candidate_tokens = len(candidate_ids)

            if candidate_tokens > max_source_tokens:
                if not kept:
                    ids = self.tokenizer(
                        candidate,
                        add_special_tokens=False,
                        truncation=True,
                        max_length=max_source_tokens,
                    ).input_ids
                    selected = clean_text(
                        self.tokenizer.decode(ids, skip_special_tokens=True, clean_up_tokenization_spaces=True)
                    )
                    selected_tokens = len(ids)
                    kept = [selected] if selected else []
                    truncated_by_tokens = True
                break

            kept.append(sent)
            selected = " ".join(kept).strip()
            selected_tokens = candidate_tokens

            if len(selected) >= min_chars:
                break

        if not selected:
            selected = raw
            selected_tokens = len(self.tokenizer(selected, add_special_tokens=False).input_ids)

        source_tokens = len(self.tokenizer(raw, add_special_tokens=False, truncation=True, max_length=max_source_tokens, verbose=False).input_ids)

        meta = {
            "source_chars": len(raw),
            "selected_chars": len(selected),
            "source_tokens": source_tokens,
            "selected_tokens": selected_tokens,
            "total_sentences": len(sentences),
            "kept_sentences": max(1, len(kept)),
            "truncated_by_tokens": bool(truncated_by_tokens),
            "target_source_chars": min_chars,
            "max_source_tokens": max_source_tokens,
        }
        return PreparedMainText(text=selected, meta=meta)

    def release(self) -> None:
        try:
            del self.model
        except Exception:
            pass
        try:
            del self.tokenizer
        except Exception:
            pass
        gc.collect()
        if torch.cuda.is_available():
            torch.cuda.empty_cache()


def run_cmd_lines(cmd: List[str]) -> List[str]:
    out = subprocess.check_output(cmd, text=True)
    return [line for line in out.splitlines() if line.strip()]


def load_benchmark_data(testset: str, langpair: str, sample_count: int) -> Tuple[List[str], List[str]]:
    # Use the current interpreter to avoid PATH issues with `sacrebleu` entrypoint.
    base_cmd = [sys.executable, "-m", "sacrebleu", "-t", testset, "-l", langpair, "--echo"]
    src = run_cmd_lines(base_cmd + ["src"])
    ref = run_cmd_lines(base_cmd + ["ref"])

    n = min(len(src), len(ref), sample_count)
    return src[:n], ref[:n]


def score_translation(preds: List[str], refs: List[str]) -> Dict[str, float]:
    bleu = BLEU().corpus_score(preds, [refs]).score
    chrf = CHRF(word_order=2).corpus_score(preds, [refs]).score
    return {"bleu": float(bleu), "chrf": float(chrf)}


def benchmark_and_select_model(
    lang: str,
    baseline_model: str,
    candidates: List[str],
    args,
    device: torch.device,
    logger: logging.Logger,
    benchmark_jsonl: str,
) -> str:
    models = []
    for m in [baseline_model] + candidates:
        if m and m not in models:
            models.append(m)

    report = {
        "timestamp": utc_iso(),
        "language": lang,
        "baseline_model": baseline_model,
        "candidate_models": candidates,
        "benchmark_specs": BENCHMARK_SPECS.get(lang, []),
        "scores": {},
    }

    specs = BENCHMARK_SPECS.get(lang, [])
    if not specs:
        selected = candidates[0] if candidates else baseline_model
        report["status"] = "no_public_pair_benchmark"
        report["selected_model"] = selected
        write_jsonl(benchmark_jsonl, report)
        logger.info("[%s] No direct public benchmark; selecting %s", lang, selected)
        return selected

    all_src: List[str] = []
    all_ref: List[str] = []
    for testset, langpair in specs:
        try:
            src, ref = load_benchmark_data(testset, langpair, args.benchmark_samples)
            if src and ref:
                all_src.extend(src)
                all_ref.extend(ref)
                logger.info("[%s] Loaded benchmark %s/%s with %d samples", lang, testset, langpair, len(src))
            else:
                logger.warning("[%s] Empty benchmark slice for %s/%s", lang, testset, langpair)
        except Exception as err:
            logger.warning("[%s] Failed benchmark load %s/%s: %s", lang, testset, langpair, err)

    if not all_src:
        selected = candidates[0] if candidates else baseline_model
        report["status"] = "benchmark_load_failed"
        report["selected_model"] = selected
        write_jsonl(benchmark_jsonl, report)
        logger.warning("[%s] Benchmark data unavailable; selecting %s", lang, selected)
        return selected

    for model_name in models:
        try:
            runner = TranslationModel(model_name=model_name, lang=lang, device=device, logger=logger)
            preds = runner.translate_texts(
                all_src,
                max_input_tokens=MAX_SOURCE_TOKENS_BY_LANG.get(lang, 360),
                max_new_tokens=220,
                batch_size=max(1, min(args.translate_batch_size, 8)),
            )
            if len(preds) != len(all_ref):
                raise RuntimeError(f"len(preds)={len(preds)} != len(ref)={len(all_ref)}")
            scores = score_translation(preds, all_ref)
            report["scores"][model_name] = scores
            logger.info("[%s] Benchmark %s -> BLEU=%.3f chrF=%.3f", lang, model_name, scores["bleu"], scores["chrf"])
        except Exception as err:
            report["scores"][model_name] = {"error": str(err)}
            logger.error("[%s] Benchmark failed for %s: %s", lang, model_name, err)
        finally:
            try:
                runner.release()
            except Exception:
                pass

    valid = [
        (m, sc)
        for m, sc in report["scores"].items()
        if isinstance(sc, dict) and "chrf" in sc and "bleu" in sc
    ]

    if not valid:
        selected = candidates[0] if candidates else baseline_model
        report["status"] = "benchmark_eval_failed"
        report["selected_model"] = selected
        write_jsonl(benchmark_jsonl, report)
        logger.warning("[%s] No valid benchmark scores; selecting %s", lang, selected)
        return selected

    valid.sort(key=lambda item: (item[1]["chrf"], item[1]["bleu"]), reverse=True)
    selected = valid[0][0]

    report["status"] = "ok"
    report["selected_model"] = selected
    if baseline_model in report["scores"] and "chrf" in report["scores"][baseline_model]:
        best_sc = report["scores"][selected]
        base_sc = report["scores"][baseline_model]
        report["delta_vs_baseline"] = {
            "bleu": round(best_sc["bleu"] - base_sc["bleu"], 4),
            "chrf": round(best_sc["chrf"] - base_sc["chrf"], 4),
        }

    write_jsonl(benchmark_jsonl, report)
    logger.info("[%s] Selected model: %s", lang, selected)
    return selected


def get_all_country_codes(db) -> List[str]:
    vals = db.sources.distinct("primary_location", {"include": True})
    out = sorted({v.strip() for v in vals if isinstance(v, str) and v.strip()})
    return out


def get_source_domains(db, lang: str, country_codes: List[str]) -> List[str]:
    query = {
        "include": True,
        "primary_location": {"$in": country_codes},
    }
    ors = []
    if lang in INTERNATIONAL_LANGS:
        ors.append({"major_international": True})
    if lang in REGIONAL_LANGS:
        ors.append({"major_regional": True})
    if ors:
        query["$or"] = ors

    domains = db.sources.distinct("source_domain", query)
    domains = sorted([d for d in domains if isinstance(d, str) and d.strip()])
    return domains


def translate_with_fallback(
    runner: TranslationModel,
    texts: List[str],
    max_input_tokens: int,
    max_new_tokens: int,
    batch_size: int,
) -> Tuple[List[Optional[str]], List[dict]]:
    issues: List[dict] = []
    try:
        out = runner.translate_texts(
            texts,
            max_input_tokens=max_input_tokens,
            max_new_tokens=max_new_tokens,
            batch_size=batch_size,
        )
        if len(out) != len(texts):
            raise RuntimeError(f"mismatched output length {len(out)} != {len(texts)}")
        return out, issues
    except Exception as err:
        issues.append({"type": "batch_failure", "error": str(err)})

    out_single: List[Optional[str]] = []
    for idx, text in enumerate(texts):
        try:
            pred = runner.translate_texts(
                [text],
                max_input_tokens=max_input_tokens,
                max_new_tokens=max_new_tokens,
                batch_size=1,
            )[0]
            out_single.append(pred)
        except Exception as err:
            out_single.append(None)
            issues.append({"type": "single_failure", "index": idx, "error": str(err)})
    return out_single, issues


def run_language(
    lang: str,
    db,
    args,
    logger: logging.Logger,
    run_version: str,
    issue_jsonl: str,
    progress_jsonl: str,
    benchmark_jsonl: str,
    country_codes: List[str],
) -> dict:
    summary = {
        "language": lang,
        "processed": 0,
        "updated": 0,
        "skipped": 0,
        "issues": 0,
        "source_domains": 0,
        "selected_model": None,
    }

    lang_info = db.languages.find_one({"iso_code": lang})
    if not lang_info:
        logger.error("[%s] No language entry found in db.languages. Skipping.", lang)
        summary["issues"] += 1
        return summary

    baseline_model = lang_info.get("huggingface_name")
    if not baseline_model:
        logger.error("[%s] Missing baseline huggingface_name in db.languages. Skipping.", lang)
        summary["issues"] += 1
        return summary

    candidates = MODEL_CANDIDATES.get(lang, [NLLB_MODEL])
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

    selected_model = benchmark_and_select_model(
        lang=lang,
        baseline_model=baseline_model,
        candidates=candidates,
        args=args,
        device=device,
        logger=logger,
        benchmark_jsonl=benchmark_jsonl,
    )
    summary["selected_model"] = selected_model

    source_domains = get_source_domains(db, lang, country_codes)
    summary["source_domains"] = len(source_domains)
    if not source_domains:
        logger.warning("[%s] No source domains found for major international/regional + all countries. Skipping.", lang)
        return summary

    logger.info("[%s] Source domains: %d", lang, len(source_domains))

    runner = TranslationModel(model_name=selected_model, lang=lang, device=device, logger=logger)

    doc_lang_values = doc_language_values(lang)

    existing_collections = set(db.list_collection_names())

    for yy, mm in iter_months(args.start_year, args.start_month, args.end_year, args.end_month):
        colname = f"articles-{yy}-{mm}"
        if colname not in existing_collections:
            continue

        collection = db[colname]
        query = {
            "source_domain": {"$in": source_domains},
            "include": True,
            "language": doc_lang_values[0] if len(doc_lang_values) == 1 else {"$in": doc_lang_values},
            "$and": [
                {"title": {"$type": "string"}},
                {"title": {"$ne": ""}},
                {"maintext": {"$type": "string"}},
                {"maintext": {"$ne": ""}},
            ],
            "$or": [
                {args.title_field: {"$exists": False}},
                {args.main_field: {"$exists": False}},
                {"translation_v2.version": {"$ne": run_version}},
            ],
        }
        projection = {"_id": 1, "title": 1, "maintext": 1, "source_domain": 1}

        cursor = collection.find(query, projection=projection).batch_size(max(50, args.batch_size * 4))
        if args.max_docs_per_month > 0:
            cursor = cursor.limit(args.max_docs_per_month)

        batch_docs: List[dict] = []
        batch_index = 0

        for doc in cursor:
            batch_docs.append(doc)
            if len(batch_docs) >= args.batch_size:
                batch_index += 1
                proc, upd, skp, iss = process_batch(
                    batch_docs,
                    lang,
                    colname,
                    runner,
                    collection,
                    args,
                    run_version,
                    selected_model,
                    issue_jsonl,
                    logger,
                )
                summary["processed"] += proc
                summary["updated"] += upd
                summary["skipped"] += skp
                summary["issues"] += iss
                write_jsonl(
                    progress_jsonl,
                    {
                        "timestamp": utc_iso(),
                        "language": lang,
                        "collection": colname,
                        "batch_index": batch_index,
                        "processed": proc,
                        "updated": upd,
                        "skipped": skp,
                        "issues": iss,
                        "selected_model": selected_model,
                    },
                )
                batch_docs = []

        if batch_docs:
            batch_index += 1
            proc, upd, skp, iss = process_batch(
                batch_docs,
                lang,
                colname,
                runner,
                collection,
                args,
                run_version,
                selected_model,
                issue_jsonl,
                logger,
            )
            summary["processed"] += proc
            summary["updated"] += upd
            summary["skipped"] += skp
            summary["issues"] += iss
            write_jsonl(
                progress_jsonl,
                {
                    "timestamp": utc_iso(),
                    "language": lang,
                    "collection": colname,
                    "batch_index": batch_index,
                    "processed": proc,
                    "updated": upd,
                    "skipped": skp,
                    "issues": iss,
                    "selected_model": selected_model,
                },
            )

        logger.info(
            "[%s] %s cumulative processed=%d updated=%d skipped=%d issues=%d",
            lang,
            colname,
            summary["processed"],
            summary["updated"],
            summary["skipped"],
            summary["issues"],
        )

    runner.release()
    return summary


def process_batch(
    docs: List[dict],
    lang: str,
    colname: str,
    runner: TranslationModel,
    collection,
    args,
    run_version: str,
    selected_model: str,
    issue_jsonl: str,
    logger: logging.Logger,
) -> Tuple[int, int, int, int]:
    processed = len(docs)
    updated = 0
    skipped = 0
    issues = 0

    titles_src: List[str] = []
    mains_src: List[str] = []
    main_meta: List[dict] = []

    for doc in docs:
        title = clean_text(doc.get("title")) or "."
        prep = runner.prepare_maintext(doc.get("maintext", ""), lang=lang, target_english_chars=args.target_english_chars)
        titles_src.append(title)
        mains_src.append(prep.text)
        main_meta.append(prep.meta)

    title_out, title_issues = translate_with_fallback(
        runner,
        titles_src,
        max_input_tokens=96,
        max_new_tokens=96,
        batch_size=args.translate_batch_size,
    )
    main_out, main_issues = translate_with_fallback(
        runner,
        mains_src,
        max_input_tokens=MAX_SOURCE_TOKENS_BY_LANG.get(lang, 360),
        max_new_tokens=args.max_new_tokens_main,
        batch_size=args.translate_batch_size,
    )

    if title_issues:
        issues += len(title_issues)
        write_jsonl(
            issue_jsonl,
            {
                "timestamp": utc_iso(),
                "language": lang,
                "collection": colname,
                "issue_type": "title_batch",
                "details": title_issues,
                "batch_doc_ids": [str(d.get("_id")) for d in docs],
            },
        )
    if main_issues:
        issues += len(main_issues)
        write_jsonl(
            issue_jsonl,
            {
                "timestamp": utc_iso(),
                "language": lang,
                "collection": colname,
                "issue_type": "main_batch",
                "details": main_issues,
                "batch_doc_ids": [str(d.get("_id")) for d in docs],
            },
        )

    ops: List[UpdateOne] = []
    now_dt = datetime.utcnow()

    for idx, doc in enumerate(docs):
        title_en = title_out[idx] if idx < len(title_out) else None
        main_en = main_out[idx] if idx < len(main_out) else None

        if not isinstance(title_en, str) or not title_en.strip() or not isinstance(main_en, str) or not main_en.strip():
            skipped += 1
            issues += 1
            write_jsonl(
                issue_jsonl,
                {
                    "timestamp": utc_iso(),
                    "language": lang,
                    "collection": colname,
                    "_id": str(doc.get("_id")),
                    "doc_id": str(doc.get("_id")),
                    "issue_type": "missing_translation_output",
                },
            )
            continue

        main_flags, main_metrics = quality_flags(
            mains_src[idx],
            main_en,
            field="main",
            min_abs_chars=40,
            short_ratio=0.08,
            check_latin_ratio=True,
        )
        title_flags, title_metrics = quality_flags(
            titles_src[idx],
            title_en,
            field="title",
            min_abs_chars=8,
            short_ratio=0.04,
            check_latin_ratio=False,
        )
        flags = main_flags + title_flags
        problem_flags = [f for f in flags if f.startswith("main_")]

        # Keep unique order
        seen = set()
        uniq_flags = []
        for f in flags:
            if f not in seen:
                uniq_flags.append(f)
                seen.add(f)

        quality_metrics = {
            "main": main_metrics,
            "title": title_metrics,
        }

        trans_meta = {
            "version": run_version,
            "model_name": selected_model,
            "source_language": normalize_doc_language(lang),
            "target_language": "en",
            "created_at": now_dt,
            "quality_flags": uniq_flags,
            "problem_flags": problem_flags,
            "quality_metrics": quality_metrics,
            "maintext_preprocess": main_meta[idx],
        }

        hist_entry = {
            "ts": now_dt,
            "version": run_version,
            "model_name": selected_model,
            "quality_flags": uniq_flags,
            "problem_flags": problem_flags,
        }

        update_doc = {
            "$set": {
                args.lang_field: "en",
                args.title_field: title_en,
                args.main_field: main_en,
                "translation_v2": trans_meta,
                "translation_v2_problem": bool(problem_flags),
            },
            "$push": {
                "translation_v2_history": {
                    "$each": [hist_entry],
                    "$slice": -20,
                }
            },
        }

        if args.dry_run:
            updated += 1
            continue

        ops.append(UpdateOne({"_id": doc["_id"]}, update_doc))

        if problem_flags:
            issues += 1
            write_jsonl(
                issue_jsonl,
                {
                    "timestamp": utc_iso(),
                    "language": lang,
                    "collection": colname,
                    "_id": str(doc.get("_id")),
                    "doc_id": str(doc.get("_id")),
                    "issue_type": "quality_flags",
                    "flags": problem_flags,
                    "all_flags": uniq_flags,
                    "quality_metrics": quality_metrics,
                },
            )

    if args.dry_run:
        return processed, updated, skipped, issues

    if ops:
        try:
            result = collection.bulk_write(ops, ordered=False)
            updated += int(result.modified_count or 0)
        except PyMongoError as err:
            issues += len(ops)
            logger.error("[%s] bulk_write failed (%s): %s", lang, colname, err)
            write_jsonl(
                issue_jsonl,
                {
                    "timestamp": utc_iso(),
                    "language": lang,
                    "collection": colname,
                    "issue_type": "bulk_write_failure",
                    "error": str(err),
                    "ops": len(ops),
                },
            )

    return processed, updated, skipped, issues


def parse_args():
    now = datetime.utcnow()
    ap = argparse.ArgumentParser(description="Run translation v2 with benchmark gating and traceable logs")
    ap.add_argument("--mongo-uri", default=os.environ.get("PIPELINE_MONGO_URI", DEFAULT_MONGO_URI))
    ap.add_argument("--db", default=os.environ.get("PIPELINE_MONGO_DB", DEFAULT_DB))
    ap.add_argument("--languages", default=",".join(DEFAULT_LANG_ORDER), help="Comma-separated language codes")

    ap.add_argument("--start-year", type=int, default=2012)
    ap.add_argument("--start-month", type=int, default=1)
    ap.add_argument("--end-year", type=int, default=now.year)
    ap.add_argument("--end-month", type=int, default=now.month)

    ap.add_argument("--batch-size", type=int, default=24, help="Mongo docs per processing batch")
    ap.add_argument("--translate-batch-size", type=int, default=8, help="Generation micro-batch size")

    ap.add_argument("--benchmark-samples", type=int, default=120, help="Samples per benchmark dataset")
    ap.add_argument("--target-english-chars", type=int, default=600)
    ap.add_argument("--max-new-tokens-main", type=int, default=320)
    ap.add_argument("--max-docs-per-month", type=int, default=0)

    ap.add_argument("--title-field", default="title_translated_v2")
    ap.add_argument("--main-field", default="maintext_translated_v2")
    ap.add_argument("--lang-field", default="language_translated_v2")

    ap.add_argument("--version", default="", help="translation_v2 version override")
    ap.add_argument("--log-dir", default="/home/ml4p/peace-machine/logs")

    ap.add_argument("--dry-run", action="store_true", help="Prepare + benchmark + log, but do not write DB updates")

    return ap.parse_args()


def main():
    args = parse_args()
    ensure_dir(args.log_dir)

    run_id = utc_ts()
    run_version = args.version.strip() or f"v2-{run_id}"

    log_file = os.path.join(args.log_dir, f"translate_v2_{run_id}.log")
    issue_jsonl = os.path.join(args.log_dir, f"translate_v2_{run_id}.issues.jsonl")
    progress_jsonl = os.path.join(args.log_dir, f"translate_v2_{run_id}.progress.jsonl")
    benchmark_jsonl = os.path.join(args.log_dir, f"translate_v2_{run_id}.benchmark.jsonl")
    summary_json = os.path.join(args.log_dir, f"translate_v2_{run_id}.summary.json")

    logger = setup_logger(log_file)
    logger.info("Run ID: %s", run_id)
    logger.info("Run version: %s", run_version)
    logger.info("Dry run: %s", args.dry_run)

    langs = [x.strip() for x in args.languages.split(",") if x.strip()]
    logger.info("Languages: %s", langs)

    db = MongoClient(args.mongo_uri)[args.db]
    country_codes = get_all_country_codes(db)
    logger.info("Included primary_location country codes found: %d", len(country_codes))

    overall = {
        "run_id": run_id,
        "run_version": run_version,
        "dry_run": args.dry_run,
        "languages": langs,
        "start": utc_iso(),
        "summaries": [],
    }

    for lang in langs:
        try:
            logger.info("----- START language=%s -----", lang)
            summary = run_language(
                lang=lang,
                db=db,
                args=args,
                logger=logger,
                run_version=run_version,
                issue_jsonl=issue_jsonl,
                progress_jsonl=progress_jsonl,
                benchmark_jsonl=benchmark_jsonl,
                country_codes=country_codes,
            )
            logger.info("----- END language=%s summary=%s -----", lang, summary)
            overall["summaries"].append(summary)
        except Exception as err:
            logger.exception("Fatal error in language=%s: %s", lang, err)
            overall["summaries"].append(
                {
                    "language": lang,
                    "processed": 0,
                    "updated": 0,
                    "skipped": 0,
                    "issues": 1,
                    "selected_model": None,
                    "fatal_error": str(err),
                }
            )

    overall["end"] = utc_iso()
    with open(summary_json, "w", encoding="utf-8") as f:
        json.dump(overall, f, indent=2, ensure_ascii=False, default=str)

    logger.info("Completed run. Summary JSON: %s", summary_json)
    logger.info("Log files: %s | %s | %s | %s", log_file, issue_jsonl, progress_jsonl, benchmark_jsonl)


if __name__ == "__main__":
    main()
