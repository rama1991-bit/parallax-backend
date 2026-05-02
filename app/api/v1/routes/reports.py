import json

from fastapi import APIRouter, Depends, HTTPException, Response

from app.core.session import get_session_id
from app.services.feed.store import (
    FeedStoreError,
    get_report,
    list_saved_reports,
    update_report_saved,
)

router = APIRouter()


def _report_to_markdown(report: dict) -> str:
    lines = [
        f"# {report.get('title') or 'Untitled report'}",
        "",
        report.get("summary") or "",
        "",
        f"Source: {report.get('source') or report.get('domain') or 'Unknown'}",
        "",
        "## Key Claims",
    ]

    claims = report.get("key_claims") or []
    if claims:
        lines.extend([f"- {claim}" for claim in claims])
    else:
        lines.append("- No claims extracted.")

    lines.extend(["", "## Narrative Framing"])
    frames = report.get("narrative_framing") or []
    if frames:
        lines.extend([f"- {frame}" for frame in frames])
    else:
        lines.append("- No framing signals extracted.")

    lines.extend(["", "## Methodology", report.get("methodology_note") or ""])
    lines.extend(["", "## Limitations"])
    lines.extend([f"- {item}" for item in report.get("limitations", [])])
    lines.append("")
    return "\n".join(lines)


@router.get("/{report_id}")
async def get_report_detail(report_id: str, session_id: str = Depends(get_session_id)):
    try:
        report = get_report(report_id, session_id=session_id)
    except FeedStoreError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    if not report:
        raise HTTPException(status_code=404, detail="Report not found")

    return report


@router.post("/{report_id}/save")
async def save_report(report_id: str, session_id: str = Depends(get_session_id)):
    try:
        report = update_report_saved(report_id, session_id=session_id, is_saved=True)
    except FeedStoreError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    if not report:
        raise HTTPException(status_code=404, detail="Report not found")

    return report


@router.post("/{report_id}/unsave")
async def unsave_report(report_id: str, session_id: str = Depends(get_session_id)):
    try:
        report = update_report_saved(report_id, session_id=session_id, is_saved=False)
    except FeedStoreError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    if not report:
        raise HTTPException(status_code=404, detail="Report not found")

    return report


@router.get("/{report_id}/export")
async def export_report(
    report_id: str,
    format: str = "json",
    session_id: str = Depends(get_session_id),
):
    try:
        report = get_report(report_id, session_id=session_id)
    except FeedStoreError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    if not report:
        raise HTTPException(status_code=404, detail="Report not found")

    if format == "markdown":
        return Response(_report_to_markdown(report), media_type="text/markdown")

    return Response(json.dumps(report, indent=2), media_type="application/json")


@router.get("")
async def saved_reports(session_id: str = Depends(get_session_id), limit: int = 50):
    try:
        reports = list_saved_reports(session_id=session_id, limit=limit)
    except FeedStoreError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    return {"reports": reports}
