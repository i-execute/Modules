import React, { useState } from 'react';

export default function LockScreen({ bot, expectedPin, onUnlock }) {
  const [pin, setPin] = useState('');
  const [error, setError] = useState('');

  const handleSubmit = (e) => {
    e.preventDefault();
    if (pin.trim() === String(expectedPin).trim()) {
      onUnlock();
    } else {
      setError('Неверный PIN-код');
      setPin('');
    }
  };

  return (
    <div style={{
      display: 'flex',
      alignItems: 'center',
      justifyContent: 'center',
      height: '100%',
      width: '100%',
      backgroundColor: 'var(--panel)'
    }}>
      <div style={{
        backgroundColor: 'var(--bg)',
        border: '1px solid var(--border)',
        borderRadius: 16,
        padding: '32px 28px',
        maxWidth: 380,
        width: '90%',
        boxShadow: '0 8px 30px rgba(0,0,0,0.12)',
        textAlign: 'center'
      }}>
        <div style={{
          width: 72,
          height: 72,
          borderRadius: '50%',
          backgroundColor: 'var(--primary)',
          color: '#fff',
          fontSize: 32,
          fontWeight: 700,
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center',
          margin: '0 auto 16px'
        }}>
          {bot?.name ? bot.name[0].toUpperCase() : 'B'}
        </div>
        <h2 style={{ fontSize: 20, marginBottom: 6 }}>{bot?.name || 'Telegram Bot'}</h2>
        <p style={{ color: 'var(--muted)', fontSize: 13, marginBottom: 20 }}>
          {bot?.username ? `@${bot.username}` : 'Приватный дамп сообщений'}
        </p>

        <form onSubmit={handleSubmit}>
          <div style={{ marginBottom: 16 }}>
            <input
              type="password"
              inputMode="numeric"
              placeholder="Введите PIN-код"
              value={pin}
              onChange={(e) => { setPin(e.target.value); setError(''); }}
              style={{
                width: '100%',
                padding: '12px 14px',
                borderRadius: 10,
                border: '1px solid var(--border)',
                backgroundColor: 'var(--panel)',
                color: 'var(--text)',
                fontSize: 16,
                textAlign: 'center',
                letterSpacing: 4
              }}
              autoFocus
            />
          </div>
          {error && (
            <div style={{ color: 'var(--danger)', fontSize: 13, marginBottom: 14 }}>
              {error}
            </div>
          )}
          <button
            type="submit"
            style={{
              width: '100%',
              padding: '12px',
              borderRadius: 10,
              backgroundColor: 'var(--primary)',
              color: '#ffffff',
              fontSize: 15,
              fontWeight: 600
            }}
          >
            Разблокировать
          </button>
        </form>
      </div>
    </div>
  );
}