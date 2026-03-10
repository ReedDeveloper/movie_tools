import jieba.analyse

from utils.date_utils import confidence_rank, normalize_release_date


HIGHLIGHT_KEYWORDS = [
    "烧脑", "反转", "治愈", "赛博朋克", "温情", "催泪", "黑色幽默", "硬核",
    "大片", "经典", "必看", "神作", "泪目", "爆笑", "细思极恐", "全程高能",
    "脑洞大开", "真实改编", "人性", "救赎", "复仇", "末日", "废土", "蒸汽朋克",
]


def extract_tags(text, top_k=5):
    if not text:
        return []

    found_highlights = [keyword for keyword in HIGHLIGHT_KEYWORDS if keyword in text]
    tfidf_tags = jieba.analyse.extract_tags(text, topK=top_k * 2)

    final_tags = found_highlights[:3]
    for tag in tfidf_tags:
        if tag not in final_tags and len(final_tags) < top_k:
            final_tags.append(tag)
    return final_tags


def build_movie_key(movie):
    douban_id = str(movie.get("douban_id", "")).strip()
    if douban_id:
        return f"douban:{douban_id}"

    title = str(movie.get("title", "")).strip().lower()
    year = str(movie.get("year", "")).strip()
    return f"title:{title}:{year}"


def normalize_list_field(value):
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, str):
        return [item.strip() for item in value.split(",") if item.strip()]
    return [str(value).strip()]


def maybe_update_release_date(movie, candidate_date, source, confidence):
    normalized = normalize_release_date(candidate_date)
    if not normalized:
        return

    current_confidence = movie.get("release_date_confidence", "unknown")
    current_rank = confidence_rank(current_confidence)
    new_rank = confidence_rank(confidence)

    if not movie.get("release_date") or new_rank >= current_rank:
        movie["release_date"] = normalized
        movie["release_date_source"] = source
        movie["release_date_confidence"] = confidence


def clean_movie_data(movie):
    movie = dict(movie)
    movie["movie_key"] = build_movie_key(movie)
    movie["rating"] = float(movie.get("rating") or 0)

    if isinstance(movie.get("year"), str) and movie["year"].isdigit():
        movie["year"] = int(movie["year"])

    genres = normalize_list_field(movie.get("genres"))
    countries = normalize_list_field(movie.get("countries"))
    tmdb_tags = normalize_list_field(movie.get("tmdb_tags"))
    smart_tags = normalize_list_field(movie.get("smart_tags"))

    if not smart_tags:
        smart_tags = extract_tags(movie.get("summary", ""))

    movie["genres"] = ", ".join(genres)
    movie["countries"] = ", ".join(countries)
    movie["tmdb_tags"] = ", ".join(tmdb_tags)
    movie["smart_tags"] = ", ".join(smart_tags)
    movie["release_date"] = normalize_release_date(movie.get("release_date"))
    movie["release_date_source"] = movie.get("release_date_source", "")
    movie["release_date_confidence"] = movie.get("release_date_confidence", "unknown")
    movie["decision_status"] = movie.get("decision_status", "pending")
    return movie
