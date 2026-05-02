alter table public.feed_cards add column if not exists session_id text;
alter table public.article_analyses add column if not exists session_id text;

create index if not exists idx_feed_cards_visible_created
    on public.feed_cards (session_id, is_dismissed, created_at desc);

create index if not exists idx_feed_cards_article_id
    on public.feed_cards (session_id, article_id);

create index if not exists idx_feed_cards_report_id
    on public.feed_cards (session_id, report_id);

create index if not exists idx_article_analyses_final_url
    on public.article_analyses (session_id, final_url);
