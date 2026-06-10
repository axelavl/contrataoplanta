from __future__ import annotations

import os


os.environ.setdefault("DB_PASSWORD", "test-password")
os.environ.setdefault("ADMIN_PASSWORD", "test-admin-password")
os.environ.setdefault("ADMIN_JWT_SECRET", "test-jwt-secret-do-not-use-in-prod")
