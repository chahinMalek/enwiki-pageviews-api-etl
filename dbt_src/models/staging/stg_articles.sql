select
    pageid,
    title,
    description,
    extract,
    wikibase_item,
    type,
    first_seen_date
from {{ source('staging', 'articles') }}
