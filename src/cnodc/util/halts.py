import typing as t


class HaltInterrupt(KeyboardInterrupt):
    pass  # pragma: no cover


class HaltFlag(t.Protocol):

    def breakpoint(self):
        self.check_continue(True)

    def check_continue(self, raise_ex: bool = True) -> bool:
        if not self._should_continue():
            if raise_ex:
                raise HaltInterrupt()
            return False
        return True

    def _should_continue(self) -> bool:
        raise NotImplementedError()  # pragma: no cover

    @staticmethod
    def iterate(iterable: t.Iterable, halt_flag=None, raise_ex: bool = True):
        if halt_flag is None:
            yield from iterable
        else:
            for x in iterable:
                if not halt_flag.check_continue(raise_ex):
                    break
                yield x
