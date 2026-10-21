import React, { useState, useEffect } from 'react';
import ChatList from './components/ChatList';
import ChatView from './components/ChatView';
import ProfileModal from './components/ProfileModal';

function getTelegramUser() {
  try {
    if (window.Telegram?.WebApp?.initDataUnsafe?.user) {
      return window.Telegram.WebApp.initDataUnsafe.user;
    }
    const raw = window.Telegram?.WebApp?.initData;
    if (raw) {
      const params = new URLSearchParams(raw);
      const userStr = params.get('user');
      if (userStr) {
        return JSON.parse(userStr);
      }
    }
  } catch (e) {}
  return null;
}

function decryptPayload(encoded, key) {
  try {
    const raw = atob(encoded);
    const keyBytes = new TextEncoder().encode(String(key));
    const out = new Uint8Array(raw.length);
    for (let i = 0; i < raw.length; i++) {
      out[i] = raw.charCodeAt(i) ^ keyBytes[i % keyBytes.length];
    }
    const jsonStr = new TextDecoder().decode(out);
    return JSON.parse(jsonStr);
  } catch (e) {
    return null;
  }
}

export default function App() {
  const [data, setData] = useState(null);
  const [activeChatId, setActiveChatId] = useState(null);
  const [profileChat, setProfileChat] = useState(null);
  const [isMobile, setIsMobile] = useState(window.innerWidth < 768);
  const [accessState, setAccessState] = useState('checking'); // 'checking', 'granted', 'denied', 'pin_prompt'
  const [denialReason, setDenialReason] = useState('');
  const [pinInput, setPinInput] = useState('');
  const [pinError, setPinError] = useState('');
  const [rawEncryptedData, setRawEncryptedData] = useState(null);

  useEffect(() => {
    if (window.Telegram?.WebApp) {
      try {
        window.Telegram.WebApp.ready();
        window.Telegram.WebApp.expand();
      } catch (e) {}
    }

    const handleResize = () => setIsMobile(window.innerWidth < 768);
    window.addEventListener('resize', handleResize);
    return () => window.removeEventListener('resize', handleResize);
  }, []);

  const resolveBotData = async () => {
    const tgUser = getTelegramUser();
    const currentUserId = tgUser?.id;

    // Direct embedded dump
    if (window.DUMP_DATA) {
      const d = window.DUMP_DATA;
      if (currentUserId && String(currentUserId) === String(d.owner_id)) {
        setData(d);
        setAccessState('granted');
        return;
      }
      if (!currentUserId) {
        setRawEncryptedData(d);
        setAccessState('pin_prompt');
        return;
      }
      setAccessState('denied');
      setDenialReason(`Доступ ограничен. Ваш ID (${currentUserId}) не является владельцем дампа.`);
      return;
    }

    // Encrypted embedded dump
    if (window.ENCRYPTED_DUMP) {
      setRawEncryptedData(window.ENCRYPTED_DUMP);
      if (currentUserId) {
        const dec = decryptPayload(window.ENCRYPTED_DUMP, currentUserId);
        if (dec && String(currentUserId) === String(dec.owner_id)) {
          setData(dec);
          setAccessState('granted');
          return;
        }
      }
      setAccessState('pin_prompt');
      return;
    }

    // Dynamic fetch based on pathname or start_param
    const pathParts = window.location.pathname.split('/').filter(Boolean);
    const startParam = window.Telegram?.WebApp?.initDataUnsafe?.start_param;
    const targetBotId = startParam || (pathParts.length > 0 ? pathParts[0] : '');

    const candidates = targetBotId ? [`/${targetBotId}/data.json`, `data.json`] : ['data.json'];
    let fetched = null;

    for (const url of candidates) {
      try {
        const res = await fetch(url);
        if (res.ok) {
          fetched = await res.json();
          break;
        }
      } catch (e) {}
    }

    if (fetched) {
      if (fetched.encrypted) {
        setRawEncryptedData(fetched.encrypted);
        if (currentUserId) {
          const dec = decryptPayload(fetched.encrypted, currentUserId);
          if (dec && String(currentUserId) === String(dec.owner_id)) {
            setData(dec);
            setAccessState('granted');
            return;
          }
        }
        setAccessState('pin_prompt');
        return;
      }

      if (currentUserId && String(currentUserId) === String(fetched.owner_id)) {
        setData(fetched);
        setAccessState('granted');
        return;
      }

      if (!currentUserId) {
        setRawEncryptedData(fetched);
        setAccessState('pin_prompt');
        return;
      }

      setAccessState('denied');
      setDenialReason(`Доступ ограничен. Ваш ID (${currentUserId}) не совпадает с владельцем дампа.`);
      return;
    }

    setAccessState('denied');
    setDenialReason('Архив диалогов не найден.');
  };

  useEffect(() => {
    resolveBotData();
  }, []);

  const handlePinSubmit = (e) => {
    e.preventDefault();
    const pin = pinInput.trim();
    if (!pin) return;

    if (rawEncryptedData) {
      if (typeof rawEncryptedData === 'string') {
        const dec = decryptPayload(rawEncryptedData, pin);
        if (dec && dec.chats) {
          setData(dec);
          setAccessState('granted');
          setPinError('');
          return;
        }
      } else if (rawEncryptedData.pin && String(rawEncryptedData.pin) === pin) {
        setData(rawEncryptedData);
        setAccessState('granted');
        setPinError('');
        return;
      }
    }

    setPinError('Неверный PIN-код или ключ владельца');
    setPinInput('');
  };

  if (accessState === 'checking') {
    return (
      <div style={{
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        height: '100%',
        backgroundColor: 'var(--bg)',
        color: 'var(--muted)',
        fontFamily: 'sans-serif'
      }}>
        Проверка доступа Telegram Mini App...
      </div>
    );
  }

  if (accessState === 'pin_prompt') {
    return (
      <div style={{
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        height: '100%',
        width: '100%',
        backgroundColor: 'var(--panel)',
        fontFamily: 'sans-serif',
        padding: 20
      }}>
        <div style={{
          backgroundColor: 'var(--bg)',
          border: '1px solid var(--border)',
          borderRadius: 16,
          padding: '32px 28px',
          maxWidth: 380,
          width: '100%',
          textAlign: 'center',
          boxShadow: '0 8px 30px rgba(0,0,0,0.12)'
        }}>
          <div style={{ fontSize: 44, marginBottom: 12 }}>🔒</div>
          <h2 style={{ fontSize: 20, marginBottom: 8, color: 'var(--text)' }}>Приватный дамп</h2>
          <p style={{ color: 'var(--muted)', fontSize: 13, lineHeight: 1.5, marginBottom: 20 }}>
            Для просмотра вне Telegram введите PIN-код владельца или откройте через Mini App:
          </p>

          <form onSubmit={handlePinSubmit}>
            <input
              type="password"
              inputMode="numeric"
              placeholder="PIN-код или ID"
              value={pinInput}
              onChange={(e) => { setPinInput(e.target.value); setPinError(''); }}
              style={{
                width: '100%',
                padding: '12px 14px',
                borderRadius: 10,
                border: '1px solid var(--border)',
                backgroundColor: 'var(--panel)',
                color: 'var(--text)',
                fontSize: 16,
                textAlign: 'center',
                letterSpacing: 3,
                marginBottom: 12
              }}
              autoFocus
            />

            {pinError && (
              <div style={{ color: 'var(--danger)', fontSize: 13, marginBottom: 12 }}>
                {pinError}
              </div>
            )}

            <button
              type="submit"
              style={{
                width: '100%',
                padding: '12px',
                backgroundColor: 'var(--primary)',
                color: '#fff',
                borderRadius: 10,
                fontSize: 14,
                fontWeight: 600,
                border: 'none',
                cursor: 'pointer'
              }}
            >
              Разблокировать
            </button>
          </form>
        </div>
      </div>
    );
  }

  if (accessState === 'denied') {
    const tgUser = getTelegramUser();
    return (
      <div style={{
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        height: '100%',
        width: '100%',
        backgroundColor: 'var(--panel)',
        fontFamily: 'sans-serif',
        padding: 20
      }}>
        <div style={{
          backgroundColor: 'var(--bg)',
          border: '1px solid var(--border)',
          borderRadius: 16,
          padding: '32px 28px',
          maxWidth: 420,
          width: '100%',
          textAlign: 'center',
          boxShadow: '0 8px 30px rgba(0,0,0,0.12)'
        }}>
          <div style={{ fontSize: 50, marginBottom: 12 }}>🔒</div>
          <h2 style={{ fontSize: 20, marginBottom: 10, color: 'var(--text)' }}>Доступ ограничен</h2>
          <p style={{ color: 'var(--muted)', fontSize: 13, lineHeight: 1.5, marginBottom: 16 }}>
            {denialReason || 'Этот дамп сообщений строго приватный и доступен исключительно его владельцу.'}
          </p>
          <div style={{
            padding: '10px 14px',
            backgroundColor: 'var(--panel)',
            borderRadius: 8,
            fontSize: 12,
            color: 'var(--muted)',
            border: '1px solid var(--border)'
          }}>
            Ваш Telegram ID: <b>{tgUser?.id || 'Не в Telegram Mini App'}</b>
          </div>
        </div>
      </div>
    );
  }

  const activeChat = (data?.chats || []).find((c) => String(c.id) === String(activeChatId));
  const activeMessages = (data?.messages && activeChatId) ? (data.messages[String(activeChatId)] || []) : [];

  return (
    <div style={{ display: 'flex', height: '100%', width: '100%', overflow: 'hidden' }}>
      {(!isMobile || !activeChatId) && (
        <div style={{ width: isMobile ? '100%' : '340px', flexShrink: 0, height: '100%' }}>
          <ChatList
            bot={data?.bot}
            chats={data?.chats || []}
            activeChatId={activeChatId}
            onSelectChat={(id) => setActiveChatId(id)}
          />
        </div>
      )}

      {(!isMobile || activeChatId) && (
        <div style={{ flex: 1, height: '100%', minWidth: 0 }}>
          <ChatView
            chat={activeChat}
            messages={activeMessages}
            onBack={isMobile ? () => setActiveChatId(null) : null}
            onOpenProfile={(c) => setProfileChat(c)}
          />
        </div>
      )}

      {profileChat && (
        <ProfileModal
          chat={profileChat}
          onClose={() => setProfileChat(null)}
        />
      )}
    </div>
  );
}
