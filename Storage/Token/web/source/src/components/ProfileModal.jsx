import React from 'react';
import Avatar from './Avatar';

export default function ProfileModal({ chat, onClose }) {
  if (!chat) return null;

  return (
    <div style={{
      position: 'fixed',
      top: 0,
      left: 0,
      right: 0,
      bottom: 0,
      backgroundColor: 'rgba(0, 0, 0, 0.45)',
      display: 'flex',
      alignItems: 'center',
      justifyContent: 'center',
      zIndex: 1000
    }}>
      <div style={{
        backgroundColor: 'var(--bg)',
        border: '1px solid var(--border)',
        borderRadius: 16,
        padding: '28px 24px',
        maxWidth: 360,
        width: '90%',
        boxShadow: '0 8px 30px rgba(0,0,0,0.2)',
        textAlign: 'center'
      }}>
        <div style={{ display: 'flex', justifyContent: 'center' }}>
          <Avatar entity={chat} size={76} fontSize={28} style={{ margin: '0 auto 16px', boxShadow: '0 4px 12px rgba(0,0,0,0.15)' }} />
        </div>

        <h3 style={{ fontSize: 18, marginBottom: 4, color: 'var(--text)' }}>
          {chat.name || `Chat ${chat.id}`}
        </h3>

        {chat.username && (
          <p style={{ color: 'var(--primary)', fontSize: 14, marginBottom: 16 }}>
            @{chat.username}
          </p>
        )}

        <div style={{
          backgroundColor: 'var(--panel)',
          borderRadius: 10,
          padding: '12px 14px',
          margin: '16px 0',
          textAlign: 'left',
          fontSize: 13,
          border: '1px solid var(--border)',
          display: 'flex',
          flexDirection: 'column',
          gap: 8
        }}>
          <div>
            <span style={{ color: 'var(--muted)' }}>Telegram ID: </span>
            <code style={{ fontWeight: 600 }}>{chat.id}</code>
          </div>
          <div>
            <span style={{ color: 'var(--muted)' }}>Сообщений в архиве: </span>
            <b>{chat.count || 0}</b>
          </div>
          {chat.date && (
            <div>
              <span style={{ color: 'var(--muted)' }}>Последнее действие: </span>
              <span>{chat.date}</span>
            </div>
          )}
        </div>

        <button
          onClick={onClose}
          style={{
            width: '100%',
            padding: '11px',
            backgroundColor: 'var(--primary)',
            color: '#fff',
            borderRadius: 8,
            fontSize: 14,
            fontWeight: 600,
            border: 'none',
            cursor: 'pointer'
          }}
        >
          Закрыть
        </button>
      </div>
    </div>
  );
}
