create table if not exists public.topics (
    id uuid primary key,
    session_id text,
    name text not null,
    description text,
    keywords jsonb not null default '[]'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists public.monitors (
    id uuid primary key,
    session_id text,
    topic_id uuid,
    name text not null,
    keywords jsonb not null default '[]'::jsonb,
    status text not null default 'active',
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

alter table public.topics add column if not exists session_id text;
alter table public.topics add column if not exists description text;
alter table public.topics add column if not exists keywords jsonb not null default '[]'::jsonb;
alter table public.topics add column if not exists created_at timestamptz not null default now();
alter table public.topics add column if not exists updated_at timestamptz not null default now();

alter table public.monitors add column if not exists session_id text;
alter table public.monitors add column if not exists topic_id uuid;
alter table public.monitors add column if not exists keywords jsonb not null default '[]'::jsonb;
alter table public.monitors add column if not exists status text not null default 'active';
alter table public.monitors add column if not exists created_at timestamptz not null default now();
alter table public.monitors add column if not exists updated_at timestamptz not null default now();

create index if not exists idx_topics_session_created
    on public.topics (session_id, created_at desc);

create index if not exists idx_monitors_session_topic
    on public.monitors (session_id, topic_id);
