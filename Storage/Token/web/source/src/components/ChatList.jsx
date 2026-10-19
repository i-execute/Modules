import React, { useState } from 'react';

export default function ChatList({ bot, chats, activeChatId, onSelectChat }) {
  const [query, setQuery] = useState('');

  const filtered = (chats || []).filter((c) => {
    const q = query.toLowerCase();
    const nameMatch = (c.name || '').toLowerCase().includes(q);
    const userMatch = (c.username || '').toLowerCase().includes(q);
    const lastMatch = (c.last || '').toLowerCase().includes(q);
    return nameMatch || userMatch || lastMatch;
  });

  return (
    <div style={{
      display: 'flex',
      flexDirection: 'column',
      height: '100%',
      backgroundColor: 'var(--panel)',
      borderRight: '1px solid var(--border)'
    }}>
      <div style={{
        padding: '12px 14px',
        borderBottom: '1px solid var(--border)',
        display: 'flex',
        alignItems: 'center',
        gap: 10
      }}>
        <div style={{
          width: 40,
          height: 40,
          borderRadius: '50%',
          backgroundColor: 'var(--primary)',
          color: '#ffffff',
          fontWeight: 700,
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center',
          flexShrink: 0
        }}>
          {bot?.name ? bot.name[0].toUpperCase() : 'B'}
        </div>
        <div style={{ overflow: 'hidden', flex: 1 }}>
          <div style={{ fontWeight: 600, fontSize: 15, whiteSpace: 'nowrap', textOverflow: 'ellipsis', overflow: 'hidden' }}>
            {bot?.name || 'Telegram Bot'}
          </div>
          <div style={{ fontSize: 12, color: 'var(--muted)' }}>
            {bot?.username ? `@${bot.username}` : ''} ({chats?.length || 0} диалогов)
          </div>
        </div>
      </div>

      <div style={{ padding: '8px 12px', borderBottom: '1px solid var(--border)' }}>
        <input
          type="search"
          placeholder="Поиск диалогов..."
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          style={{
            width: '100%',
            padding: '8px 12px',
            borderRadius: 8,
            border: '1px solid var(--border)',
            backgroundColor: 'var(--bg)',
            color: 'var(--text)',
            fontSize: 13
          }}
        />
      </div>

      <div style={{ flex: 1, overflowY: 'auto' }}>
        {filtered.length === 0 ? (
          <div style={{ padding: 20, textAlign: 'center', color: 'var(--muted)', fontSize: 13 }}>
            Диалоги не найдены
          </div>
        ) : (
          filtered.map((c) => {
            const isActive = String(c.id) === String(activeChatId);
            const initials = (c.name || 'U').trim()[0].toUpperCase();
            return (
              <div
                key={c.id}
                onClick={() => onSelectChat(c.id)}
                style={{
                  display: 'flex',
                  alignItems: 'center',
                  padding: '10px 12px',
                  gap: 10,
                  cursor: 'pointer',
                  backgroundColor: isActive ? 'var(--hover)' : 'transparent',
                  borderBottom: '1px solid var(--border)',
                  transition: 'background-color 0.15s ease'
                }}
              >
                <div style={{
                  width: 44,
                  height: 44,
                  borderRadius: '50%',
                  backgroundColor: '#708499',
                  color: '#fff',
                  fontWeight: 600,
                  fontSize: 16,
                  display: 'flex',
                  alignItems: 'center',
                  justifyContent: 'center',
                  flexShrink: 0
                }}>
                  {initials}
                </div>
                <div style={{ flex: 1, minWidth: 0 }}>
                  <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 2 }}>
                    <span style={{ fontWeight: 600, fontSize: 14, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                      {c.name || `Chat ${c.id}`}
                    </span>
                    <span style={{ fontSize: 11, color: 'var(--muted)', flexShrink: 0, marginLeft: 6 }}>
                      {c.date ? c.date.slice(11, 16) : ''}
                    </span>
                  </div>
                  <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                    <span style={{ fontSize: 12, color: 'var(--muted)', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                      {c.last || (c.username ? `@${c.username}` : '')}
                    </span>
                    {c.count > 0 && (
                      <span style={{
                        backgroundColor: 'var(--badge)',
                        color: '#fff',
                        fontSize: 11,
                        borderRadius: 10,
                        padding: '1px 6px',
                        fontWeight: 600,
                        marginLeft: 4
                      }}>
                        {c.count}
                      </span>
                    )}
                  </div>
                </div>
              </div>
            );
          })
        )}
      </div>
    </div>
  );
}
