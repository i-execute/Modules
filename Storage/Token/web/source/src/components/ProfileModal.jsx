import React from 'react';

export default function ProfileModal({ chat, onClose }) {
  if (!chat) return null;

  const initials = (chat.name || 'U').trim()[0].toUpperCase();

  return (
    <div
      onClick={onClose}
      style={{
        position: 'fixed',
        inset: 0,
        backgroundColor: 'rgba(0,0,0,0.5)',
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        zIndex: 100,
        padding: 16
      }}
    >
      <div
        onClick={(e) => e.stopPropagation()}
        style={{
          backgroundColor: 'var(--bg)',
          borderRadius: 16,
          padding: '24px',
          width: '100%',
          maxWidth: 360,
          border: '1px solid var(--border)',
          boxShadow: '0 10px 30px rgba(0,0,0,0.2)'
        }}
      >
        <div style={{ textAlign: 'center', marginBottom: 20 }}>
          <div style={{
            width: 72,
            height: 72,
            borderRadius: '50%',
            backgroundColor: '#708499',
            color: '#fff',
            fontSize: 28,
            fontWeight: 700,
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            margin: '0 auto 12px'
          }}>
            {initials}
          </div>
          <h3 style={{ fontSize: 18, marginBottom: 4 }}>{chat.name || `Chat ${chat.id}`}</h3>
          <div style={{ fontSize: 13, color: 'var(--muted)' }}>
            {chat.username ? `@${chat.username}` : `Chat ID: ${chat.id}`}
          </div>
        </div>

        <div style={{
          backgroundColor: 'var(--panel)',
          borderRadius: 10,
          padding: '12px 16px',
          marginBottom: 16,
          display: 'flex',
          flexDirection: 'column',
          gap: 10,
          fontSize: 13
        }}>
          <div style={{ display: 'flex', justifyContent: 'space-between' }}>
            <span style={{ color: 'var(--muted)' }}>Telegram ID</span>
            <span style={{ fontWeight: 600 }}>{chat.id}</span>
          </div>
          <div style={{ display: 'flex', justifyContent: 'space-between' }}>
            <span style={{ color: 'var(--muted)' }}>Сообщений</span>
            <span style={{ fontWeight: 600 }}>{chat.count || 0}</span>
          </div>
          {chat.username && (
            <div style={{ display: 'flex', justifyContent: 'space-between' }}>
              <span style={{ color: 'var(--muted)' }}>Ссылка</span>
              <a
                href={`https://t.me/${chat.username}`}
                target="_blank"
                rel="noreferrer"
                style={{ color: 'var(--primary)', textDecoration: 'none', fontWeight: 500 }}
              >
                t.me/{chat.username}
              </a>
            </div>
          )}
        </div>

        <button
          onClick={onClose}
          style={{
            width: '100%',
            padding: '10px',
            backgroundColor: 'var(--hover)',
            borderRadius: 8,
            fontSize: 14,
            fontWeight: 500,
            color: 'var(--text)'
          }}
        >
          Закрыть
        </button>
      </div>
    </div>
  );
}
