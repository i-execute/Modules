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

function serverlessCall(name, input) {
  return new Promise((resolve, reject) => {
    const srv = window.Telegram?.WebApp?.Serverless;
    if (!srv || typeof srv.call !== 'function') {
      reject(new Error('serverless api unavailable'));
      return;
    }
    let settled = false;
    const done = (err, res) => {
      if (settled) return;
      settled = true;
      if (err) reject(err); else resolve(res);
    };
    try {
      const maybePromise = srv.call(name, input || {}, (res, err) => done(err || null, res));
      if (maybePromise && typeof maybePromise.then === 'function') {
        maybePromise.then((res) => done(null, res), (err) => done(err));
      }
    } catch (e) {
      done(e);
    }
    setTimeout(() => done(new Error('serverless timeout')), 10000);
  });
}

export default function App() {
  const [data, setData] = useState(null);
  const [activeChatId, setActiveChatId] = useState(null);
  const [profileChat, setProfileChat] = useState(null);
  const [isMobile, setIsMobile] = useState(window.innerWidth < 768);
  const [accessState, setAccessState] = useState('checking');
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

  const resolveBotData = async () => {
    const tgUser = getTelegramUser();

    const pathParts = window.location.pathname.split('/').filter(Boolean);
    const startParam = window.Telegram?.WebApp?.initDataUnsafe?.start_param || '';
    const botId = window.DUMP_BOT_ID || startParam || (pathParts.length > 0 ? pathParts[0] : '');
    const ownerId = window.DUMP_OWNER_ID || null;

    let visibility = window.DUMP_VISIBILITY || null;
    let encrypted = typeof window.ENCRYPTED_DUMP === 'string' ? window.ENCRYPTED_DUMP : null;
    let payload = null;

    if (!encrypted && !visibility && !botId && pathParts.length === 0 && !startParam) {
      visibility = null;
    }

    if (window.DUMP_PAYLOAD && typeof window.DUMP_PAYLOAD === 'object') {
      payload = window.DUMP_PAYLOAD;
      visibility = visibility || 'public';
    }

    if (!encrypted && !payload) {
      const candidates = botId
        ? [`/${botId}/data.json`, `${botId}/data.json`, 'data.json']
        : ['data.json'];
      let fetched = null;
      for (const url of candidates) {
        try {
          const res = await fetch(url, { cache: 'no-store' });
          if (res.ok) {
            const text = await res.text();
            if (text.trim().startsWith('{')) {
              fetched = JSON.parse(text);
              break;
            }
          }
        } catch (e) {}
      }
      if (fetched) {
        visibility = fetched.visibility || (fetched.encrypted ? 'private' : 'public');
        if (fetched.encrypted) encrypted = fetched.encrypted;
        payload = fetched.payload || (fetched.chats ? fetched : null);
      }
    }

    if (!visibility) {
      visibility = encrypted ? 'private' : 'public';
    }

    if (visibility === 'public') {
      if (payload && payload.chats) {
        setData(payload);
        setAccessState('granted');
      } else {
        setAccessState('denied');
        setDenialReason('Архив диалогов не найден или повреждён.');
      }
      return;
    }

    if (!encrypted) {
      setAccessState('denied');
      setDenialReason('Архив диалогов не найден.');
      return;
    }

    if (!tgUser?.id) {
      setAccessState('denied');
      setDenialReason('Приватный дамп открывается только внутри Telegram Mini App — запросите ссылку у владельца.');
      return;
    }

    const dec = decryptPayload(encrypted, tgUser.id);
    const ownerMatch = dec && String(dec.owner_id ?? ownerId ?? tgUser.id) === String(tgUser.id);

    if (!ownerMatch) {
      setAccessState('denied');
      setDenialReason(`Этот дамп принадлежит другому пользователю. Ваш ID (${tgUser.id}) не является владельцем.`);
      return;
    }

    try {
      await serverlessCall(`b${botId}`, {});
    } catch (e) {}

    setData(dec);
    setAccessState('granted');
  };

  useEffect(() => {
    resolveBotData();
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
            Ваш Telegram ID: <b>{getTelegramUser()?.id || 'Не в Telegram Mini App'}</b>
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
