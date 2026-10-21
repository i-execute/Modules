import React from 'react';

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
  const parts = String(name).trim().split(/\s+/);
  if (parts.length >= 2) {
    return (parts[0][0] + parts[1][0]).toUpperCase();
  }
  return parts[0].slice(0, 2).toUpperCase();
}

export default function Avatar({ entity, size = 40, fontSize = 15, style = {} }) {
  const src = entity?.avatar;
  if (src) {
    return (
      <img
        src={src}
        alt=""
        draggable={false}
        style={{
          width: size,
          height: size,
          borderRadius: '50%',
          objectFit: 'cover',
          flexShrink: 0,
          display: 'block',
          userSelect: 'none',
          ...style
        }}
      />
    );
  }
  return (
    <div
      style={{
        width: size,
        height: size,
        borderRadius: '50%',
        backgroundColor: getAvatarColor(entity?.id || entity?.name),
        color: '#ffffff',
        fontWeight: 600,
        fontSize,
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        flexShrink: 0,
        userSelect: 'none',
        letterSpacing: '0.5px',
        ...style
      }}
    >
      {getInitials(entity?.name || entity?.username)}
    </div>
  );
}
