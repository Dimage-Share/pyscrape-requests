from __future__ import annotations
import os
from app import create_app
from core.logger import setup_logging


def main():
    setup_logging()
    app = create_app({
        'SECRET_KEY': os.environ.get('APP_SECRET', 'dev-key')
    })
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=True)


if __name__ == '__main__':
    main()
