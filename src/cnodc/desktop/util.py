import cnodc.desktop.translations as i18n


class TranslatableException(Exception):

    def __init__(self, key, **kwargs):
        self.key = key
        self.kwargs = kwargs

    @property
    def message(self):
        return i18n.get_text(self.key, **self.kwargs)

    def __str__(self):
        return self.message


class StopAction(Exception):
    pass
