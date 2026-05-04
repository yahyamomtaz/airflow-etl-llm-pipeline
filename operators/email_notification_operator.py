import json
import logging
import urllib.request
from airflow.models import BaseOperator, Variable
from airflow.utils.email import send_email
from datetime import datetime

log = logging.getLogger(__name__)


class EmailNotificationOperator(BaseOperator):
    """
    Sends pipeline execution notifications via email and optionally Slack/PagerDuty.

    Airflow Variables consumed (all optional):
      SLACK_WEBHOOK_URL      – Incoming Webhook URL; omit to disable Slack
      PAGERDUTY_ROUTING_KEY  – Events API v2 routing key; omit to disable PagerDuty
                               PagerDuty alert is only triggered when failed books > 0.

    :param to_emails: List of recipient email addresses
    :param subject: Email subject (supports templating)
    """

    template_fields = ('subject', 'to_emails')

    def __init__(
        self,
        to_emails,
        subject='Il sito web è stato aggiornato',
        include_summary=True,
        include_failed_books=True,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.to_emails = to_emails if isinstance(to_emails, list) else [to_emails]
        self.subject = subject
        self.include_summary = include_summary
        self.include_failed_books = include_failed_books

    def execute(self, context):
        ti = context["ti"]

        summary = ti.xcom_pull(
            task_ids="mark_books_processed",
            key="book_summary",
        ) or {}

        processed = summary.get("processed", [])
        partial   = summary.get("partial", [])
        failed    = summary.get("failed", [])

        html = self._build_html_email(processed, partial, failed)
        if self.to_emails:
            self._send_email(html)
            self.log.info("Email sent to %s", self.to_emails)

        self._send_slack(processed, partial, failed)
        self._send_pagerduty(failed, context)

        return {"sent_to": self.to_emails, "books_count": len(processed)}

    # ── Email ────────────────────────────────────────────────────────────────

    def _build_html_email(self, processed, partial, failed):
        if processed:
            books_html = "<ul style='list-style: none; padding: 0;'>"
            for book_number in processed:
                books_html += (
                    f"<li style='padding: 10px; margin: 5px 0; background: #f0fdf4;"
                    f" border-left: 3px solid #10b981; border-radius: 4px;'>"
                    f"<strong style='color: #1f2937;'>{book_number}</strong></li>"
                )
            books_html += "</ul>"
        else:
            books_html = "<p style='color: #6b7280; font-style: italic;'>No new books were fully processed.</p>"

        partial_html = ""
        if partial:
            partial_html = (
                f"<div style='margin-top: 20px; padding: 15px; background: #fffbeb;"
                f" border-left: 3px solid #f59e0b; border-radius: 4px;'>"
                f"<strong style='color: #92400e;'>Partially Processed ({len(partial)} books)</strong>"
                f"<p style='color: #78716c; margin: 5px 0 0 0; font-size: 0.9em;'>"
                f"{', '.join(partial)}</p></div>"
            )

        failed_html = ""
        if failed:
            failed_html = (
                f"<div style='margin-top: 20px; padding: 15px; background: #fef2f2;"
                f" border-left: 3px solid #ef4444; border-radius: 4px;'>"
                f"<strong style='color: #991b1b;'>Failed ({len(failed)} books)</strong>"
                f"<p style='color: #78716c; margin: 5px 0 0 0; font-size: 0.9em;'>"
                f"{', '.join(failed)}</p></div>"
            )

        return f"""<!DOCTYPE html>
<html>
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0"></head>
<body style='font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Arial, sans-serif;
             line-height: 1.6; color: #1f2937; margin: 0; padding: 0; background-color: #f3f4f6;'>
  <div style='max-width: 600px; margin: 40px auto; background: white; border-radius: 12px;
              box-shadow: 0 4px 6px rgba(0,0,0,0.1); overflow: hidden;'>
    <div style='padding: 30px;'>
      <h3 style='color: #374151; font-size: 18px; margin-top: 0; margin-bottom: 15px;'>
        Il sito web è stato aggiornato e sono stati aggiunti i seguenti libri:
      </h3>
      {books_html}
      {partial_html}
      {failed_html}
    </div>
    <div style='background: #f9fafb; padding: 20px 30px; border-top: 1px solid #e5e7eb; text-align: center;'>
      <p style='margin: 0; color: #6b7280; font-size: 12px;'>
        This is an automated message.<br>
        {datetime.now().strftime('%Y-%m-%d at %H:%M:%S')}
      </p>
    </div>
  </div>
</body>
</html>"""

    def _send_email(self, html_content):
        send_email(to=self.to_emails, subject=self.subject, html_content=html_content)

    # ── Slack ────────────────────────────────────────────────────────────────

    def _send_slack(self, processed, partial, failed):
        webhook_url = Variable.get('SLACK_WEBHOOK_URL', default_var='')
        if not webhook_url:
            return

        status_emoji = "✅" if not failed else "⚠️"
        lines = [
            f"{status_emoji} *Magic Pipeline* — {datetime.now().strftime('%Y-%m-%d %H:%M')}",
            f"*Processed:* {len(processed)}  |  *Partial:* {len(partial)}  |  *Failed:* {len(failed)}",
        ]
        if processed:
            lines.append(f"Books added: {', '.join(processed)}")
        if failed:
            lines.append(f":x: Failed: {', '.join(failed)}")

        payload = json.dumps({"text": "\n".join(lines)}).encode()
        try:
            req = urllib.request.Request(
                webhook_url,
                data=payload,
                headers={"Content-Type": "application/json"},
            )
            urllib.request.urlopen(req, timeout=10)
            self.log.info("Slack notification sent")
        except Exception as e:
            self.log.warning("Slack notification failed: %s", e)

    # ── PagerDuty ────────────────────────────────────────────────────────────

    def _send_pagerduty(self, failed, context):
        routing_key = Variable.get('PAGERDUTY_ROUTING_KEY', default_var='')
        if not routing_key or not failed:
            return

        dag_id  = context.get('dag').dag_id if context.get('dag') else 'unknown'
        run_id  = context.get('run_id', 'unknown')
        payload = json.dumps({
            "routing_key": routing_key,
            "event_action": "trigger",
            "payload": {
                "summary": f"Magic pipeline: {len(failed)} book(s) failed — {', '.join(failed)}",
                "severity": "error",
                "source": f"airflow/{dag_id}",
                "custom_details": {"dag_id": dag_id, "run_id": run_id, "failed_books": failed},
            },
        }).encode()

        try:
            req = urllib.request.Request(
                "https://events.pagerduty.com/v2/enqueue",
                data=payload,
                headers={"Content-Type": "application/json"},
            )
            urllib.request.urlopen(req, timeout=10)
            self.log.info("PagerDuty alert triggered for %d failed book(s)", len(failed))
        except Exception as e:
            self.log.warning("PagerDuty alert failed: %s", e)
