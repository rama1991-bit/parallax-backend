create index if not exists idx_article_analyses_session_created
    on public.article_analyses (session_id, created_at desc);
