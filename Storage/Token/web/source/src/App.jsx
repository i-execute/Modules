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
  const [accessState, setAccessState] = useState('checking'); // 'checking', 'granted', 'denied'
  const [denialReason, setDenialReason] = useState('');

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

  useEffect(() => {
    const tgUser = getTelegramUser();
    const currentUserId = tgUser?.id;

    // Check if running inside Telegram Mini App
    if (!tgUser || !currentUserId) {
      setAccessState('denied');
      setDenialReason('Доступ разрешен только внутри Telegram Mini App. Прямой просмотр через браузер запрещен.');
      return;
    }

    // Try resolving dump data from window.DUMP_DATA or window.ENCRYPTED_DUMP
    let resolvedData = null;

    if (window.ENCRYPTED_DUMP) {
      resolvedData = decryptPayload(window.ENCRYPTED_DUMP, currentUserId);
    } else if (window.DUMP_DATA) {
      resolvedData = window.DUMP_DATA;
    }

    if (resolvedData) {
      if (String(currentUserId) === String(resolvedData.owner_id)) {
        setData(resolvedData);
        setAccessState('granted');
      } else {
        setAccessState('denied');
        setDenialReason(`Доступ ограничен. Ваш ID (${currentUserId}) не совпадает с ID владельца дампа.`);
      }
      return;
    }

    // If data not yet in window, fetch data.json
    const pathParts = window.location.pathname.split('/').filter(Boolean);
    const subPath = pathParts.length > 0 ? pathParts[0] : '';
    const dataUrl = subPath ? `/${subPath}/data.json` : 'data.json';

    fetch(dataUrl)
      .then((r) => r.json())
      .then((d) => {
        if (d && d.encrypted) {
          const dec = decryptPayload(d.encrypted, currentUserId);
          if (dec && String(currentUserId) === String(dec.owner_id)) {
            setData(dec);
            setAccessState('granted');
            return;
          }
        } else if (d && String(currentUserId) === String(d.owner_id)) {
          setData(d);
          setAccessState('granted');
          return;
        }
        setAccessState('denied');
        setDenialReason(`Доступ ограничен. Ваш ID (${currentUserId}) не совпадает с ID владельца.`);
      })
      .catch(() => {
        setAccessState('denied');
        setDenialReason('Архив диалогов не найден или доступ заблокирован.');
      });
  }, []);

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
            bot={data.bot}
            chats={data.chats || []}
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
