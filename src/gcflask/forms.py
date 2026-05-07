from flask_wtf import FlaskForm
from flask_wtf.file import FileField


class GCFlaskForm(FlaskForm):

    def __init__(self, *args, **kwargs):
        self._with_file_upload = False
        super().__init__(*args, **kwargs)
        for name in self._fields:
            if isinstance(self._fields[name], FileField):
                self._with_file_upload = True
                break

    def validate_on_submit(self, extra_validators=None):
        if super().validate_on_submit(extra_validators):
            return True
        elif self.errors:
            ...
        return False
