import base64

from inkbox_claude import media


def test_extension_prefers_url_then_content_type():
    assert media._extension_for("image/jpeg", "https://x/a/photo.PNG?sig=1") == ".png"
    assert media._extension_for("image/jpeg", "https://x/a/blob?sig=1") == ".jpg"
    assert media._extension_for(None, "https://x/a/blob") == ".bin"


def test_inbound_media_note_lists_paths():
    note = media.inbound_media_note([
        {"path": "/m/sms-0.jpg", "content_type": "image/jpeg"},
        {"path": "/m/sms-1.pdf", "content_type": "application/pdf"},
    ])
    assert "Read tool" in note
    assert "/m/sms-0.jpg (image/jpeg)" in note
    assert "/m/sms-1.pdf (application/pdf)" in note


def test_inbound_media_note_empty_when_nothing_saved():
    assert media.inbound_media_note([]) == ""


def test_file_to_email_attachment_base64s_local_file(tmp_path):
    f = tmp_path / "report.txt"
    f.write_bytes(b"hello world")
    att = media.file_to_email_attachment(str(f))
    assert att["filename"] == "report.txt"
    assert att["content_type"] == "text/plain"
    assert base64.b64decode(att["content_base64"]) == b"hello world"


def test_file_to_email_attachment_unknown_type_falls_back(tmp_path):
    f = tmp_path / "blob.weirdext"
    f.write_bytes(b"\x00\x01")
    att = media.file_to_email_attachment(str(f))
    assert att["content_type"] == "application/octet-stream"
