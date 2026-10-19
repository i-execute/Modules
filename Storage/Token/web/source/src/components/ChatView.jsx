import React, { useEffect, useRef } from 'react';

export default function ChatView({ chat, messages, onBack, onOpenProfile }) {
  const bottomRef = useRef(null);

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [messages, chat?.id]);

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

  const initials = (chat.name || 'U').trim()[0].toUpperCase();

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
        <div style={{ display: 'flex', alignItems: 'center', gap: 10, minWidth: 0 }}>
          {onBack && (
            <button
              onClick={onBack}
              style={{
                fontSize: 18,
                padding: '4px 8px',
                color: 'var(--primary)'
              }}
            >
              ←
            </button>
          )}
          <div style={{
            width: 38,
            height: 38,
            borderRadius: '50%',
            backgroundColor: '#708499',
            color: '#fff',
            fontWeight: 600,
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
            <div style={{ fontSize: 12, color: 'var(--muted)' }}>
              {chat.username ? `@${chat.username}` : `ID: ${chat.id}`}
            </div>
          </div>
        </div>

        <button
          onClick={() => onOpenProfile(chat)}
          style={{
            padding: '6px 12px',
            backgroundColor: 'var(--hover)',
            borderRadius: 6,
            fontSize: 13,
            color: 'var(--primary)',
            fontWeight: 500
          }}
        >
          Профиль
        </button>
      </div>

      <div style={{
        flex: 1,
        overflowY: 'auto',
        padding: '16px 14px',
        display: 'flex',
        flexDirection: 'column',
        gap: 8
      }}>
        {(messages || []).map((m) => {
          const isOut = Boolean(m.out);
          return (
            <div
              key={m.id}
              style={{
                display: 'flex',
                justifyContent: isOut ? 'flex-end' : 'flex-start',
                width: '100%'
              }}
            >
              <div style={{
                maxWidth: '75%',
                padding: '8px 12px',
                borderRadius: 12,
                backgroundColor: isOut ? 'var(--out-bubble)' : 'var(--in-bubble)',
                border: '1px solid var(--border)',
                boxShadow: '0 1px 2px rgba(0,0,0,0.06)',
                wordBreak: 'break-word',
                fontSize: 14,
                lineHeight: 1.4
              }}>
                {m.text && (
                  <div style={{ whiteSpace: 'pre-wrap', marginBottom: 4 }}>
                    {m.text}
                  </div>
                )}
                {m.media && (
                  <div style={{
                    fontSize: 12,
                    color: 'var(--primary)',
                    backgroundColor: 'rgba(51, 144, 236, 0.1)',
                    padding: '4px 8px',
                    borderRadius: 6,
                    marginBottom: 4
                  }}>
                    📎 {m.media}
                  </div>
                )}
                <div style={{
                  fontSize: 11,
                  color: 'var(--muted)',
                  textAlign: 'right',
                  marginTop: 2
                }}>
                  {m.date ? m.date.slice(11, 16) : ''}
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
        Режим просмотра архива диалогов
      </div>
    </div>
  );
}
