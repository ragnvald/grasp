from __future__ import annotations

MANAGED_DATA_LANGUAGE_NOT_SET_LABEL = "Not set"
MANAGED_DATA_LANGUAGE_OPTIONS = tuple(
    sorted(
        [
            "Afrikaans",
            "Albanian",
            "Amharic",
            "Arabic",
            "Armenian",
            "Azerbaijani",
            "Basque",
            "Belarusian",
            "Bengali",
            "Bosnian",
            "Bulgarian",
            "Burmese",
            "Catalan",
            "Chinese",
            "Croatian",
            "Czech",
            "Danish",
            "Dutch",
            "English",
            "Estonian",
            "Filipino",
            "Finnish",
            "French",
            "Galician",
            "Georgian",
            "German",
            "Greek",
            "Gujarati",
            "Hebrew",
            "Hindi",
            "Hungarian",
            "Icelandic",
            "Indonesian",
            "Irish",
            "Italian",
            "Japanese",
            "Kannada",
            "Kazakh",
            "Korean",
            "Kurdish",
            "Latvian",
            "Lithuanian",
            "Macedonian",
            "Malay",
            "Malayalam",
            "Marathi",
            "Mongolian",
            "Nepali",
            "Norwegian",
            "Persian",
            "Polish",
            "Portuguese",
            "Punjabi",
            "Romanian",
            "Russian",
            "Serbian",
            "Sinhala",
            "Slovak",
            "Slovenian",
            "Somali",
            "Spanish",
            "Swahili",
            "Swedish",
            "Tamil",
            "Telugu",
            "Thai",
            "Turkish",
            "Ukrainian",
            "Urdu",
            "Uzbek",
            "Vietnamese",
            "Welsh",
            "Yoruba",
            "Zulu",
        ],
        key=str.casefold,
    )
)


def normalize_managed_data_language(value: str | None) -> str:
    text = str(value or "").strip()
    if not text or text.casefold() == MANAGED_DATA_LANGUAGE_NOT_SET_LABEL.casefold():
        return ""
    for language in MANAGED_DATA_LANGUAGE_OPTIONS:
        if language.casefold() == text.casefold():
            return language
    return ""


def display_managed_data_language(value: str | None) -> str:
    return normalize_managed_data_language(value) or MANAGED_DATA_LANGUAGE_NOT_SET_LABEL
