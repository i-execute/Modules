import React, { useState, useEffect } from 'react';
import ChatList from './components/ChatList';
import ChatView from './components/ChatView';
import ProfileModal from './components/ProfileModal';
import LockScreen from './components/LockScreen';

export default function App() {
  const [data, setData] = useState(() => window.DUMP_DATA || null);
  const [isUnlocked, setIsUnlocked] = useState(false);
  const [activeChatId, setActiveChatId] = useState(null);
  const [profileChat, setProfileChat] = useState(null);
  const [isMobile, setIsMobile] = useState(window.innerWidth < 768);
  const [accessDenied, setAccessDenied] = useState(false);

  useEffect(() => {
    if (window.Telegram?.WebApp) {
      window.Telegram.WebApp.ready();
      window.Telegram.WebApp.expand();
    }

    const handleResize = () => setIsMobile(window.innerWidth < 768);
    window.addEventListener('resize', handleResize);
    return () => window.removeEventListener('resize', handleResize);
  }, []);

  useEffect(() => {
    if (!data) {
      fetch('data.json')
        .then((r) => r.json())
        .then((d) => setData(d))
        .catch(() => {});
    }
  }, [data]);

  useEffect(() => {
    if (!data) return;

    if (!data.is_private) {
      setIsUnlocked(true);
      return;
    }

    const initData = window.Telegram?.WebApp?.initDataUnsafe;
    const currentUserId = initData?.user?.id;

    if (currentUserId) {
      if (String(currentUserId) === String(data.owner_id)) {
        setIsUnlocked(true);
      } else {
        setAccessDenied(true);
      }
    }
  }, [data]);

  if (!data) {
    return (
      <div style={{
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        height: '100%',
        color: 'var(--muted)'
      }}>
        Загрузка архива диалогов...
      </div>
    );
  }

  if (accessDenied) {
    const currentUserId = window.Telegram?.WebApp?.initDataUnsafe?.user?.id;
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
          textAlign: 'center',
          boxShadow: '0 8px 30px rgba(0,0,0,0.12)'
        }}>
          <div style={{ fontSize: 40, marginBottom: 12 }}>🔒</div>
          <h2 style={{ fontSize: 20, marginBottom: 8 }}>Доступ ограничен</h2>
          <p style={{ color: 'var(--muted)', fontSize: 13, lineHeight: 1.5 }}>
            Этот дамп является приватным и доступен только владельцу бота.
          </p>
          <div style={{ marginTop: 16, fontSize: 12, color: 'var(--muted)' }}>
            Ваш ID: <code>{currentUserId || 'Unknown'}</code>
          </div>
        </div>
      </div>
    );
  }

  if (data.is_private && !isUnlocked) {
    return (
      <LockScreen
        bot={data.bot}
        expectedPin={data.pin}
        onUnlock={() => setIsUnlocked(true)}
      />
    );
  }

  const activeChat = (data.chats || []).find((c) => String(c.id) === String(activeChatId));
  const activeMessages = (data.messages && activeChatId) ? (data.messages[String(activeChatId)] || []) : [];

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
