import React, { useEffect, useRef } from 'react';

const AVATAR_COLORS = [
  '#e17076', '#faa774', '#a695e7', '#7bc862', '#6ec9cb', '#65aadd', '#ee7aae'
];

function getAvatarColor(idOrName) {
  let hash = 0;
  const str = String(idOrName || 'user');
  for (let i = 0; i < str.length; i++) {
    hash = (hash * 31 + str.charCodeAt(i)) >>> 0;
  }
  return AVATAR_COLORS[hash % AVATAR_COLORS.length];
}

function getInitials(name) {
  if (!name) return 'U';
  const parts = name.trim().split(/\s+/);
  if (parts.length >= 2) {
    return (parts[0][0] + parts[1][0]).toUpperCase();
  }
  return parts[0].slice(0, 2).toUpperCase();
}

function renderMarkdownTables(raw) {
  if (!raw || !raw.includes('|')) return raw;
  const regex = /((?:^|\n)\|[^\n]+\|\n\|[-:\s|]+\|\n(?:\|[^\n]+\|\n?)+)/g;
  return raw.replace(regex, (match) => {
    const lines = match.trim().split('\n');
    if (lines.length < 3) return match;
    const headers = lines[0].split('|').slice(1, -1).map((s) => s.trim());
    const rows = lines.slice(2).map((l) => l.split('|').slice(1, -1).map((s) => s.trim()));
    const thead = headers.map((h) => `<th>${h}</th>`).join('');
    const tbody = rows.map((r) => `<tr>${r.map((c) => `<td>${c}</td>`).join('')}</tr>`).join('');
    return `<table class="tg-table"><thead><tr>${thead}</tr></thead><tbody>${tbody}</tbody></table>`;
  });
}

export default function ChatView({ chat, messages, onBack, onOpenProfile }) {
  const bottomRef = useRef(null);

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [messages, chat?.id]);

  const handleSpoilerClick = (e) => {
    if (e.target.classList.contains('spoiler')) {
      e.target.classList.toggle('revealed');
    }
  };

  if (!chat) {
    return (
      <div style={{
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        height: '100%',
        color: 'var(--muted)',
        backgroundColor: 'var(--bg)',
        fontSize: 14
      }}>
        Выберите диалог слева для чтения переписки
      </div>
    );
  }

  const initials = getInitials(chat.name);
  const avatarColor = getAvatarColor(chat.id || chat.name);

  return (
    <div style={{
      display: 'flex',
      flexDirection: 'column',
      height: '100%',
      backgroundColor: 'var(--bg)'
    }}>
      <div style={{
        padding: '10px 14px',
        borderBottom: '1px solid var(--border)',
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'space-between',
        backgroundColor: 'var(--panel)'
      }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 12, minWidth: 0 }}>
          {onBack && (
            <button
              onClick={onBack}
              style={{
                fontSize: 18,
                padding: '4px 8px',
                color: 'var(--primary)',
                background: 'none',
                border: 'none',
                cursor: 'pointer'
              }}
            >
              ←
            </button>
          )}
          <div style={{
            width: 40,
            height: 40,
            borderRadius: '50%',
            backgroundColor: avatarColor,
            color: '#fff',
            fontWeight: 600,
            fontSize: 15,
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            flexShrink: 0
          }}>
            {initials}
          </div>
          <div style={{ overflow: 'hidden' }}>
            <div style={{ fontWeight: 600, fontSize: 15, whiteSpace: 'nowrap', textOverflow: 'ellipsis', overflow: 'hidden' }}>
              {chat.name || `Chat ${chat.id}`}
            </div>
            <div style={{ fontSize: 12, color: 'var(--muted)', marginTop: 2 }}>
              {chat.username ? `@${chat.username}` : `ID: ${chat.id}`}
            </div>
          </div>
        </div>

        <button
          onClick={() => onOpenProfile(chat)}
          style={{
            padding: '7px 14px',
            backgroundColor: 'var(--hover)',
            borderRadius: 8,
            fontSize: 13,
            color: 'var(--primary)',
            fontWeight: 500,
            border: 'none',
            cursor: 'pointer'
          }}
        >
          Профиль
        </button>
      </div>

      <div
        onClick={handleSpoilerClick}
        style={{
          flex: 1,
          overflowY: 'auto',
          padding: '16px 14px',
          display: 'flex',
          flexDirection: 'column',
          gap: 10
        }}
      >
        {(messages || []).map((m) => {
          const isOut = Boolean(m.out);
          const senderName = isOut ? 'Бот' : (chat.name || `User ${chat.id}`);
          const senderColor = isOut ? 'var(--primary)' : avatarColor;

          const renderedHtml = m.html ? renderMarkdownTables(m.html) : (m.text ? renderMarkdownTables(m.text) : '');

          return (
            <div
              key={m.id}
              style={{
                display: 'flex',
                justifyContent: isOut ? 'flex-end' : 'flex-start',
                alignItems: 'flex-end',
                gap: 8,
                width: '100%'
              }}
            >
              {!isOut && (
                <div style={{
                  width: 30,
                  height: 30,
                  borderRadius: '50%',
                  backgroundColor: avatarColor,
                  color: '#fff',
                  fontWeight: 600,
                  fontSize: 11,
                  display: 'flex',
                  alignItems: 'center',
                  justifyContent: 'center',
                  flexShrink: 0,
                  marginBottom: 2
                }}>
                  {initials}
                </div>
              )}

              <div style={{
                maxWidth: '80%',
                padding: '8px 12px',
                borderRadius: isOut ? '14px 14px 2px 14px' : '14px 14px 14px 2px',
                backgroundColor: isOut ? 'var(--out-bubble)' : 'var(--in-bubble)',
                border: '1px solid var(--border)',
                boxShadow: '0 1px 2px rgba(0,0,0,0.06)',
                wordBreak: 'break-word',
                fontSize: 14,
                lineHeight: 1.45
              }}>
                <div style={{
                  fontSize: 12,
                  fontWeight: 600,
                  color: senderColor,
                  marginBottom: 4
                }}>
                  {senderName}
                </div>

                {m.reply_to && (
                  <div style={{
                    borderLeft: '2px solid var(--primary)',
                    paddingLeft: 8,
                    marginBottom: 6,
                    fontSize: 12,
                    color: 'var(--muted)',
                    backgroundColor: 'rgba(51, 144, 236, 0.06)',
                    padding: '3px 6px',
                    borderRadius: '0 4px 4px 0'
                  }}>
                    ↳ Ответ на сообщение #{m.reply_to}
                  </div>
                )}

                {renderedHtml ? (
                  <div
                    style={{ whiteSpace: 'pre-wrap', marginBottom: 4 }}
                    dangerouslySetInnerHTML={{ __html: renderedHtml }}
                  />
                ) : null}

                {m.media && (
                  <div style={{
                    fontSize: 12,
                    color: 'var(--primary)',
                    backgroundColor: 'rgba(51, 144, 236, 0.1)',
                    padding: '6px 10px',
                    borderRadius: 8,
                    marginBottom: 4,
                    display: 'flex',
                    alignItems: 'center',
                    gap: 6
                  }}>
                    <span>📎</span>
                    <span>{m.media}</span>
                  </div>
                )}

                <div style={{
                  fontSize: 11,
                  color: 'var(--muted)',
                  textAlign: 'right',
                  marginTop: 2,
                  display: 'flex',
                  justifyContent: 'flex-end',
                  alignItems: 'center',
                  gap: 4
                }}>
                  <span>{m.date ? m.date.slice(11, 16) : ''}</span>
                  {isOut && <span style={{ color: 'var(--primary)' }}>✓✓</span>}
                </div>
              </div>
            </div>
          );
        })}
        <div ref={bottomRef} />
      </div>

      <div style={{
        padding: '10px 14px',
        borderTop: '1px solid var(--border)',
        backgroundColor: 'var(--panel)',
        color: 'var(--muted)',
        fontSize: 13,
        textAlign: 'center'
      }}>
        Архив диалогов доступен только для чтения
      </div>
    </div>
  );
}
