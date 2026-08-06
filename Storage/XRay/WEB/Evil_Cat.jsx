const { useEffect, useRef, useState } = React;

const EVIL_CAT_JSON = "https://raw.githubusercontent.com/i-execute/Modules/main/Storage/XRay/Animations/Evil_Cat.json";
const THEME_MP3 = "https://raw.githubusercontent.com/i-execute/Modules/main/Storage/XRay/Media/Evil_Cat_theme.mp3";

function App() {
  const holder = useRef(null);
  const audio = useRef(null);
  const [sound, setSound] = useState(false);
  const [mutedByUser, setMutedByUser] = useState(false);

  const ensureAudio = async () => {
    if (mutedByUser) return false;
    if (!audio.current) {
      audio.current = new Audio(THEME_MP3);
      audio.current.loop = true;
      audio.current.volume = 0.16;
    }
    try { await audio.current.play(); setSound(true); return true; }
    catch (_) { return false; }
  };

  useEffect(() => {
    let animation;
    let dead = false;
    const enableFromInteraction = () => { ensureAudio(); };
    window.addEventListener("pointerdown", enableFromInteraction, { once: true, passive: true });
    window.addEventListener("keydown", enableFromInteraction, { once: true });
    (async () => {
      try {
        if (!window.lottie) {
          await new Promise((resolve, reject) => {
            const script = document.createElement("script");
            script.src = "https://cdnjs.cloudflare.com/ajax/libs/lottie-web/5.12.2/lottie.min.js";
            script.onload = resolve;
            script.onerror = reject;
            document.head.appendChild(script);
          });
        }
        const response = await fetch(EVIL_CAT_JSON, { cache: "no-store" });
        if (!response.ok) throw Error("evil_cat_" + response.status);
        const data = await response.json();
        if (!dead && holder.current) {
          animation = window.lottie.loadAnimation({ container: holder.current, renderer: "svg", loop: true, autoplay: true, animationData: data });
        }
      } catch (_) {
        if (holder.current) holder.current.textContent = "◉";
      }
    })();
    return () => { dead = true; if (animation) animation.destroy(); };
  }, []);

  const toggleSound = async (event) => {
    event.stopPropagation();
    if (sound) {
      if (audio.current) audio.current.pause();
      setSound(false);
      setMutedByUser(true);
      return;
    }
    setMutedByUser(false);
    if (!audio.current) {
      audio.current = new Audio(THEME_MP3);
      audio.current.loop = true;
      audio.current.volume = 0.16;
    }
    try { await audio.current.play(); setSound(true); } catch (_) {}
  };

  return React.createElement("main", { className: "cover", onPointerDown: ensureAudio },
    React.createElement("style", null, `
      :root{--black:#060506;--charcoal:#100d10;--ash:#aaa1a4;--paper:#ede8e8;--red:#d51825;--dim-red:#5e1017;--line:rgba(237,232,232,.14)}
      *{box-sizing:border-box}.cover{position:relative;min-height:100svh;overflow:hidden;display:flex;flex-direction:column;background:radial-gradient(ellipse 75% 52% at 50% 45%,#210b10 0%,#0e090b 46%,var(--black) 100%);color:var(--paper);font-family:ui-sans-serif,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif}.cover:before{content:"";position:absolute;inset:0;pointer-events:none;opacity:.19;background-image:repeating-linear-gradient(0deg,transparent 0,transparent 3px,rgba(255,255,255,.06) 4px),radial-gradient(circle at 50% 50%,transparent 20%,rgba(0,0,0,.56) 95%)}.cover:after{content:"";position:absolute;inset:-30%;pointer-events:none;background:conic-gradient(from 120deg at 50% 50%,transparent,rgba(213,24,37,.07),transparent 28%);animation:drift 10s linear infinite}@keyframes drift{to{transform:rotate(1turn)}}
      .cover__top{position:relative;z-index:1;display:flex;justify-content:space-between;align-items:center;padding:calc(18px + env(safe-area-inset-top)) 22px 12px;color:var(--ash);font:10px/1 ui-monospace,SFMono-Regular,Menlo,monospace;letter-spacing:.18em;text-transform:uppercase}.cover__signal{display:flex;align-items:center;gap:8px}.cover__dot{width:5px;height:5px;border-radius:50%;background:var(--red);box-shadow:0 0 10px var(--red)}
      .cover__hero{position:relative;z-index:1;display:grid;place-items:center;flex:1;min-height:0;padding:0 28px}.aura{position:absolute;width:min(58vw,290px);aspect-ratio:1;border-radius:50%;background:radial-gradient(circle,rgba(218,20,36,.42) 0,rgba(126,11,22,.22) 31%,rgba(35,5,9,.08) 54%,transparent 73%);filter:blur(3px);animation:breathe 3.8s ease-in-out infinite}@keyframes breathe{50%{transform:scale(1.1);opacity:.72}}.cat{position:relative;width:min(49vw,238px);height:min(38svh,310px);z-index:1;filter:drop-shadow(0 0 25px rgba(213,24,37,.36))}.cat:before{content:"";position:absolute;z-index:2;inset:15% 17% 14%;background:#070607;border-radius:48% 48% 42% 42%;clip-path:polygon(8% 18%,0 0,28% 12%,50% 4%,72% 12%,100% 0,92% 18%,100% 46%,86% 91%,64% 100%,36% 100%,14% 91%,0 46%);box-shadow:inset 0 -18px 28px rgba(255,255,255,.025)}.cat:after{content:"";position:absolute;z-index:3;top:45%;left:31%;width:38%;height:8%;background:radial-gradient(ellipse at 18% 50%,#fff 0 8%,#e30b1d 11% 27%,transparent 31%),radial-gradient(ellipse at 82% 50%,#fff 0 8%,#e30b1d 11% 27%,transparent 31%);filter:drop-shadow(0 0 8px #e30b1d)}.cat svg{position:absolute;inset:0;display:block;width:100%!important;height:100%!important;opacity:.46}
      .cover__footer{position:relative;z-index:1;display:flex;align-items:flex-end;justify-content:space-between;gap:14px;margin:0 18px;padding:16px 4px calc(22px + env(safe-area-inset-bottom));border-top:1px solid var(--line)}.cover__copy{display:grid;gap:6px}.cover__title{margin:0;font:500 clamp(23px,6.4vw,32px)/1.02 Georgia,"Times New Roman",serif;letter-spacing:-.04em}.cover__subtitle{margin:0;color:var(--ash);font:10px/1.45 ui-monospace,SFMono-Regular,Menlo,monospace;letter-spacing:.07em;text-transform:uppercase}.sound{appearance:none;min-width:78px;min-height:42px;padding:0 14px;border:1px solid rgba(213,24,37,.52);border-radius:0;background:rgba(26,7,10,.75);color:var(--paper);font:10px/1 ui-monospace,SFMono-Regular,Menlo,monospace;letter-spacing:.1em;text-transform:uppercase;box-shadow:inset 0 0 18px rgba(213,24,37,.1)}.sound:active{transform:scale(.96)}.sound[data-active="true"]{border-color:var(--red);color:#fff;box-shadow:inset 0 0 21px rgba(213,24,37,.2),0 0 18px rgba(213,24,37,.14)}@media(prefers-reduced-motion:reduce){.cover:after,.aura{animation:none}}
    `),
    React.createElement("header", { className: "cover__top" },
      React.createElement("span", null, "midnight channel"),
      React.createElement("span", { className: "cover__signal" }, React.createElement("i", { className: "cover__dot" }), "online")
    ),
    React.createElement("section", { className: "cover__hero", "aria-label": "Evil cat cover" },
      React.createElement("div", { className: "aura" }),
      React.createElement("div", { className: "cat", ref: holder })
    ),
    React.createElement("footer", { className: "cover__footer" },
      React.createElement("div", { className: "cover__copy" },
        React.createElement("h1", { className: "cover__title" }, "The night is watching."),
        React.createElement("p", { className: "cover__subtitle" }, "private channel · stay in the shadows")
      ),
      React.createElement("button", { className: "sound", "data-active": sound, onClick: toggleSound, "aria-label": sound ? "Turn sound off" : "Turn sound on" }, sound ? "sound on" : "sound off")
    )
  );
}

window.App = App;
