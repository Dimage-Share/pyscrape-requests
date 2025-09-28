class Console:
    
    @staticmethod
    def select(prompt: str, options: list[str]) -> str:
        while True:
            try:
                raw = input(f"{prompt} [ {'/'.join(options + ['q(uit)'])} ] : ").strip()
            except (EOFError, KeyboardInterrupt):  # noqa: PERF203
                print()
                return 0
            if not raw: continue
            ans = raw.lower()
            if ans in ('q', 'quit'): return 'quit'
            if ans in options: return ans
    
    @staticmethod
    def confirm(prompt: str) -> bool:
        while True:
            try:
                raw = input(f"{prompt} [ y/n/q(uit) ] : ").strip().lower()
            except (EOFError, KeyboardInterrupt):  # noqa: PERF203
                print()
                return 0
            if not raw: continue
            if raw in ('q', 'quit'): return False
            if raw in ('y', 'yes'): return True
            if raw in ('n', 'no'): return False
    
    @staticmethod
    def input_str(prompt: str) -> str:
        while True:
            try:
                raw = input(f"{prompt} : ").strip()
            except (EOFError, KeyboardInterrupt):  # noqa: PERF203
                print()
                return 0
            if not raw: continue
            return str(raw)
    
    @staticmethod
    def input_int(prompt: str) -> int:
        while True:
            try:
                raw = input(f"{prompt} : ").strip()
            except (EOFError, KeyboardInterrupt):  # noqa: PERF203
                print()
                return 0
            if not raw: continue
            if raw.isdigit() and int(raw) >= 0:
                return int(raw)
