import email
import pathlib
import smtplib
import ssl
import typing as t

import jinja2
from autoinject import injector
import zirconium as zr
import threading
import zrlog

from medsutil.exceptions import exception_kwargs_for_email, CodedError


@injector.injectable
class DelayedEmailController(t.Protocol):
    def send_delayed(self, kwargs: dict) -> bool: ...



@injector.injectable_global
class EmailController:

    config: zr.ApplicationConfig = None
    delayed: DelayedEmailController = None

    @injector.construct
    def __init__(self):
        self._log = zrlog.get_logger("medsutil.email")
        self._connect_args: dict[str, str | int | None | ssl.SSLContext] = {
            "host": self.config.as_str(("email", "host"), default=""),
            "port": self.config.as_int(("email", "port"), default=0),
            'local_hostname': self.config.as_str(("email", "local_hostname"), default=None),
            "timeout": self.config.as_int(("email", "timeout_seconds"), default=5.0),
        }
        self._login_args: dict[str, str | None] = {
            "user": self.config.as_str(("email", "username"), default=None),
            "password": self.config.as_str(("email", "password"), default=None),
        }
        self._use_ssl: bool = self.config.as_bool(("email", "use_ssl"), default=False)
        self._start_tls: bool = self.config.as_bool(("email", "start_tls"), default=False) and not self._use_ssl
        self._from_email: str = self.config.as_str(("email", "send_from"), default="no-reply@example.com")
        self._dummy_send: bool = self.config.as_bool(("email", "no_send"), default=False) or not self._connect_args['host']
        self.admin_emails: list[str] = self.config.as_list(("email", "admin_emails"), default=[])
        extra_template_folders = self.config.as_list(("email", "template_folders"), default=None)
        if extra_template_folders:
            extra_template_folders = [
                pathlib.Path(x)
                for x in reversed(extra_template_folders)
            ]
        else:
            extra_template_folders = []
        base_path = pathlib.Path(__file__).absolute().parent / ".email_templates"
        self._email_jinja_env = jinja2.Environment(
            loader=jinja2.FileSystemLoader([base_path, *extra_template_folders]),
            autoescape=jinja2.select_autoescape()
        )
        self._template_cache = {}
        self._lock = threading.Lock()
        if self._use_ssl:
            self._connect_args['context'] = ssl.create_default_context()

    def send_error_report(self, ex: BaseException, immediate: bool = True):
        self.send_template(
            "error",
            to_emails=self.admin_emails,
            immediate=immediate,
            **exception_kwargs_for_email(ex)
        )

    def send_template(self,
                      template_name: str,
                      template_lang: t.Literal["en", "fr"],
                      to_emails: list[str] | str | None = None,
                      cc_emails: list[str] | str | None = None,
                      bcc_emails: list[str] | str | None = None,
                      immediate: bool = False,
                      **template_args: t.Any):
        subject, message_txt, message_html = self._get_template_content(template_name, template_lang, template_args)
        return self.send_email(
            subject,
            message_txt,
            message_html,
            to_emails,
            cc_emails,
            bcc_emails,
            immediate
        )

    def _get_template_content(self, name: str, lang: t.Literal["en", "fr"], kwargs: dict[str, t.Any]) -> tuple[str, str, str]:
        message_txt = self._render_template_content(name, lang, "txt", kwargs)
        message_html = self._render_template_content(name, lang, "html", kwargs)
        subject = self._render_template_content(name + ".subject", lang, "txt", kwargs).strip("\r\n\t ")
        return (subject or f"{name}"), message_txt, message_html

    def _render_template_content(self, name: str, lang: t.Literal["en", "fr"], extension: str, kwargs: dict[str, t.Any]) -> str:
        file_name_options = [
            f"{name}.{lang}.{extension}",
            f"{name}.{extension}"
        ]
        try:
            template = self._email_jinja_env.select_template(file_name_options)
            return template.render(**kwargs)
        except jinja2.TemplatesNotFound:
            return ''

    def send_email(self,
                   subject: str,
                   message_txt: str | None = None,
                   message_html: str | None = None,
                   to_emails: list[str] | str | None = None,
                   cc_emails: list[str] | str | None = None,
                   bcc_emails: list[str] | str | None = None,
                   immediate: bool = False) -> bool:
        kwargs = {
            "to_emails": to_emails,
            "subject": subject,
            "message_txt": message_txt,
            "message_html": message_html,
            "cc_emails": cc_emails,
            "bcc_emails": bcc_emails
        }
        if immediate:
            try:
                return self.direct_send_email(**kwargs)
            except Exception:
                return self.delayed_send_email(**kwargs)
        else:
            return self.delayed_send_email(**kwargs)

    def delayed_send_email(self, **kwargs) -> bool:
        return self.delayed.send_delayed(kwargs)

    def direct_send_email(self,
                          to_emails: list[str] | str | None,
                          subject: str,
                          message_txt: str = None,
                          message_html: str = None,
                          cc_emails: list[str] | str | None = None,
                          bcc_emails: list[str] | str | None = None,
                          _no_output: bool = False) -> bool:
        # Build message
        to_addrs = self._standardize_email_list(to_emails)
        if cc_emails:
            to_addrs.extend(self._standardize_email_list(cc_emails))
        if bcc_emails:
            to_addrs.extend(self._standardize_email_list(bcc_emails))
        msg = email.message.EmailMessage()
        msg['Subject'] = subject
        msg['To'] = self._standardize_email_list(to_emails)
        if cc_emails:
            msg['CC'] = self._standardize_email_list(cc_emails)
        msg['From'] = self._from_email
        msg.set_content(message_txt)
        if message_html:
            msg.add_alternative(message_html, subtype='html')
        if not self._dummy_send:
            return self._send_smtp_message(msg, to_addrs)
        return True

    def _send_smtp_message(self, msg: email.message.EmailMessage, to_addrs: list[str]):
        # Actually send it
        if not to_addrs:
            return False
        with self._lock:
            try:
                smtp = smtplib.SMTP_SSL if self._use_ssl else smtplib.SMTP
                with smtp(**self._connect_args) as smtp:
                    if self._start_tls:
                        smtp.starttls(context=ssl.create_default_context())
                    if self._login_args['user'] or self._login_args['password']:
                        smtp.login(**self._login_args)
                    smtp.send_message(msg, to_addrs=to_addrs)
                    return True
            except (
                    smtplib.SMTPConnectError,
                    smtplib.SMTPServerDisconnected,
                    TimeoutError) as ex:
                raise CodedError(str(ex), 1000, code_space="SMTP", is_transient=True) from ex

            except Exception as ex:
                raise CodedError(str(ex), 2000, code_space="SMTP") from ex

    def _standardize_email_list(self, emails: list[str] | str | None) -> list[str]:
        if emails is None:
            return []
        if isinstance(emails, str):
            return [emails]
        return list(emails)
