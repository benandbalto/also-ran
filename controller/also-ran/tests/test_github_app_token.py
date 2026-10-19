import base64
import importlib.util
import io
import json
import pathlib
import subprocess
import sys
import tempfile
import unittest
import urllib.error
from unittest import mock


CONTROLLER_DIR = pathlib.Path(__file__).parents[1]
sys.path.insert(0, str(CONTROLLER_DIR))
SPEC = importlib.util.spec_from_file_location(
    "github_app_token", CONTROLLER_DIR / "github_app_token.py"
)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


def unbase64url(value):
    return base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))


class Response(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def granted(token="ghs_minted", permissions=None):
    return Response(json.dumps({
        "token": token,
        "expires_at": "2026-10-02T08:00:00Z",
        "repository_selection": "selected",
        "permissions": dict(permissions or MODULE.PERMISSIONS, metadata="read"),
    }).encode())


class GitHubAppTokenTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = pathlib.Path(self.directory.name)
        self.key = self.root / "app.pem"
        subprocess.run(
            ["openssl", "genpkey", "-algorithm", "RSA", "-pkeyopt", "rsa_keygen_bits:2048",
             "-out", str(self.key)],
            check=True, capture_output=True,
        )
        self.public = self.root / "app.pub"
        subprocess.run(
            ["openssl", "pkey", "-in", str(self.key), "-pubout", "-out", str(self.public)],
            check=True, capture_output=True,
        )
        self.output = self.root / "github-token"

    def argv(self):
        return ["--app-id", "12345", "--installation-id", "678",
                "--private-key-file", str(self.key), "--output", str(self.output)]

    def test_jwt_is_rs256_signed_and_short_lived(self):
        jwt = MODULE.app_jwt("12345", str(self.key), now=1_000_000)
        header, claims, signature = jwt.split(".")
        self.assertEqual(json.loads(unbase64url(header)), {"alg": "RS256", "typ": "JWT"})
        self.assertEqual(
            json.loads(unbase64url(claims)),
            {"iat": 999_940, "exp": 1_000_480, "iss": "12345"},
        )
        signature_file = self.root / "signature"
        signature_file.write_bytes(unbase64url(signature))
        verified = subprocess.run(
            ["openssl", "dgst", "-sha256", "-verify", str(self.public),
             "-signature", str(signature_file)],
            input=f"{header}.{claims}".encode(), capture_output=True,
        )
        self.assertEqual(verified.returncode, 0, verified.stderr)

    def test_unreadable_key_is_an_error(self):
        with self.assertRaisesRegex(MODULE.TokenError, "could not sign"):
            MODULE.app_jwt("12345", str(self.root / "missing.pem"))

    def test_mints_a_read_only_token_and_replaces_the_file(self):
        self.output.write_text("ghs_old\n")
        with mock.patch.object(MODULE.urllib.request, "urlopen",
                               return_value=granted()) as urlopen, \
                mock.patch.object(sys, "stdout", io.StringIO()) as stdout:
            self.assertEqual(MODULE.main(self.argv()), 0)
        request = urlopen.call_args.args[0]
        self.assertEqual(request.get_method(), "POST")
        self.assertEqual(
            request.full_url, "https://api.github.com/app/installations/678/access_tokens"
        )
        self.assertTrue(request.get_header("Authorization").startswith("Bearer ey"))
        self.assertEqual(json.loads(request.data), {"permissions": MODULE.PERMISSIONS})
        self.assertEqual(self.output.read_text(), "ghs_minted\n")
        self.assertEqual(self.output.stat().st_mode & 0o777, 0o640)
        self.assertEqual([p.name for p in self.root.iterdir() if p.name.startswith(".token-")], [])
        logged = json.loads(stdout.getvalue())
        self.assertEqual(logged["result"], "ok")
        self.assertEqual(logged["expires_at"], "2026-10-02T08:00:00Z")
        self.assertNotIn("ghs_minted", stdout.getvalue())

    def test_refusal_keeps_the_previous_token(self):
        self.output.write_text("ghs_old\n")
        refusal = urllib.error.HTTPError(
            "https://api.github.com", 422, "Unprocessable", {},
            io.BytesIO(b'{"message":"The permissions requested are not granted to this installation."}'),
        )
        with mock.patch.object(MODULE.urllib.request, "urlopen", side_effect=refusal), \
                mock.patch.object(sys, "stderr", io.StringIO()) as stderr:
            self.assertEqual(MODULE.main(self.argv()), 1)
        self.assertEqual(self.output.read_text(), "ghs_old\n")
        self.assertIn("HTTP 422", stderr.getvalue())
        self.assertIn("not granted", stderr.getvalue())

    def test_a_token_with_fewer_permissions_is_not_written(self):
        with mock.patch.object(
            MODULE.urllib.request, "urlopen",
            return_value=granted(permissions={"actions": "read"}),
        ), mock.patch.object(sys, "stderr", io.StringIO()) as stderr:
            self.assertEqual(MODULE.main(self.argv()), 1)
        self.assertFalse(self.output.exists())
        self.assertIn("was granted", stderr.getvalue())
        self.assertNotIn("ghs_minted", stderr.getvalue())


if __name__ == "__main__":
    unittest.main()
