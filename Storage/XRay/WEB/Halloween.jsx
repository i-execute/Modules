const { useEffect, useRef, useState } = React;

const PUMPKIN_JSON = "https://raw.githubusercontent.com/i-execute/Modules/main/Storage/XRay/Animations/Pumpkin.json";
const THEME_MP3 = "https://raw.githubusercontent.com/i-execute/Modules/main/Storage/Media/Halloween_theme.mp3";

function App() {
  const holder = useRef(null);
  const audio = useRef(null);
  const [sound, setSound] = useState(false);

  useEffect(() => {
    let animation;
    let cancelled = false;
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
        const response = await fetch(PUMPKIN_JSON, { cache: "force-cache" });
        if (!response.ok) throw Error("pumpkin_" + response.status);
        const data = await response.json();
        if (!cancelled && holder.current) {
          animation = window.lottie.loadAnimation({ container: holder.current, renderer: "svg", loop: true, autoplay: true, animationData: data });
        }
      } catch (_) {
        if (holder.current) holder.current.textContent = "🎃";
      }
    })();
    return () => { cancelled = true; if (animation) animation.destroy(); };
  }, []);

  const toggleSound = async () => {
    if (!audio.current) {
      audio.current = new Audio(THEME_MP3);
      audio.current.loop = true;
      audio.current.volume = 0.18;
    }
    if (sound) { audio.current.pause(); setSound(false); }
    else { try { await audio.current.play(); setSound(true); } catch (_) {} }
  };

  return React.createElement("main", { className: "cover" },
    React.createElement("style", null, `
      :root{--ink:#f3eee5;--muted:#b5a99b;--line:rgba(255,238,202,.2);--orange:#f0a24d;--green:#a5bd68}
      *{box-sizing:border-box}.cover{min-height:100svh;position:relative;overflow:hidden;display:flex;flex-direction:column;background:radial-gradient(ellipse 70% 48% at 50% 48%,#332116 0%,#15110f 47%,#080908 100%);color:var(--ink);font-family:Georgia,"Times New Roman",serif}.cover:before{content:"";position:absolute;inset:0;pointer-events:none;background:repeating-linear-gradient(90deg,transparent 0,transparent 53px,rgba(255,229,184,.018) 54px),linear-gradient(180deg,rgba(0,0,0,.12),rgba(0,0,0,.45))}.cover__top{position:relative;z-index:1;display:flex;align-items:center;justify-content:space-between;padding:calc(18px + env(safe-area-inset-top)) 22px 16px;font-family:ui-monospace,SFMono-Regular,Menlo,monospace;font-size:10px;letter-spacing:.16em;text-transform:uppercase;color:var(--muted)}.cover__signal{display:flex;align-items:center;gap:8px}.cover__dot{width:6px;height:6px;border-radius:50%;background:var(--green);box-shadow:0 0 12px var(--green)}.cover__hero{position:relative;z-index:1;flex:1;display:grid;place-items:center;min-height:0;padding:0 24px}.cover__moon{position:absolute;width:min(73vw,390px);aspect-ratio:1;border-radius:50%;background:radial-gradient(circle at 48% 42%,rgba(255,218,140,.16),rgba(236,131,58,.05) 45%,transparent 70%);filter:blur(1px)}.pumpkin{position:relative;width:min(78vw,420px);height:min(54svh,440px);filter:drop-shadow(0 28px 34px rgba(0,0,0,.53));z-index:1}.pumpkin svg{width:100%!important;height:100%!important}.cover__footer{position:relative;z-index:1;display:flex;align-items:flex-end;justify-content:space-between;gap:18px;padding:18px 22px calc(24px + env(safe-area-inset-bottom));border-top:1px solid var(--line)}.cover__copy{display:grid;gap:5px}.cover__title{margin:0;font-size:clamp(22px,6vw,31px);font-weight:400;line-height:1;letter-spacing:-.035em}.cover__subtitle{margin:0;color:var(--muted);font:11px/1.45 ui-monospace,SFMono-Regular,Menlo,monospace;letter-spacing:.05em}.sound{appearance:none;flex:0 0 auto;min-width:70px;padding:10px 12px;border:1px solid var(--line);border-radius:999px;background:rgba(18,14,11,.58);color:var(--ink);font:10px/1 ui-monospace,SFMono-Regular,Menlo,monospace;letter-spacing:.08em;text-transform:uppercase}.sound:active{transform:scale(.96)}@media(prefers-reduced-motion:reduce){.pumpkin svg{animation:none!important}}
    `),
    React.createElement("header", { className: "cover__top" },
      React.createElement("span", null, "midnight channel"),
      React.createElement("span", { className: "cover__signal" }, React.createElement("i", { className: "cover__dot" }), "online")
    ),
    React.createElement("section", { className: "cover__hero", "aria-label": "Halloween cover" },
      React.createElement("div", { className: "cover__moon" }),
      React.createElement("div", { className: "pumpkin", ref: holder })
    ),
    React.createElement("footer", { className: "cover__footer" },
      React.createElement("div", { className: "cover__copy" },
        React.createElement("h1", { className: "cover__title" }, "The lantern is awake."),
        React.createElement("p", { className: "cover__subtitle" }, "A quiet place beyond the dark.")
      ),
      React.createElement("button", { className: "sound", onClick: toggleSound, "aria-label": sound ? "Mute ambient sound" : "Play ambient sound" }, sound ? "mute" : "sound")
    )
  );
}

window.App = App;
