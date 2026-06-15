from inkbox_claude.prompts import build_channel_prompt, frame_inbound, strip_markdown


def test_frame_inbound_tags_channel_and_sender():
    assert frame_inbound("imessage", {"sender": "+15551234567"}, "hi").startswith(
        "[iMessage from +15551234567]"
    )
    assert frame_inbound("sms", {"sender": "+15551234567"}, "yo").startswith(
        "[Text message (SMS) from +15551234567]"
    )
    # Email carries its subject into the tag.
    framed = frame_inbound("email", {"sender": "a@b.com", "subject": "Deploy?"}, "body")
    assert framed.startswith("[Email from a@b.com]")
    assert "Subject: Deploy?" in framed
    # Voice has no sender tag but flags speech.
    assert frame_inbound("voice", {}, "what's up").startswith("[Spoken live on a phone call")
    # The body always survives intact.
    assert frame_inbound("imessage", {"sender": "x"}, "the message").endswith("the message")


def test_channel_prompt_mentions_identity_and_dir():
    text = build_channel_prompt(
        project_dir="/srv/app",
        identity_handle="dev-agent",
        email_address="dev-agent@inkbox.ai",
        phone_number="+15551234567",
    )
    assert "/srv/app" in text
    assert "dev-agent@inkbox.ai" in text
    assert "jargon" in text.lower()
    assert "AskUserQuestion" in text


def test_strip_markdown():
    raw = "**Done!** Ran `npm test`:\n```\nall green\n```\nSee [docs](https://x.y)."
    flat = strip_markdown(raw)
    assert "**" not in flat
    assert "`" not in flat
    assert "docs (https://x.y)" in flat
