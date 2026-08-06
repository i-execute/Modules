const { useEffect, useRef, useState } = React;

const EVIL_CAT_JSON = "https://raw.githubusercontent.com/i-execute/Modules/main/Storage/XRay/Animations/Evil_Cat.json";
const THEME_MP3 = "https://raw.githubusercontent.com/i-execute/Modules/main/Storage/XRay/Media/Evil_Cat_theme.mp3";

function App() {
  const holder = useRef(null);
  const audio = useRef(null);
  const mutedByUser = useRef(false);
  const [sound, setSound] = useState(false);
  const [awake, setAwake] = useState(false);

  const startSound = async () => {
    if (mutedByUser.current) return false;
    if (!audio.current) {
      audio.current = new Audio(THEME_MP3);
      audio.current.loop = true;
      audio.current.volume = 0.14;
    }
    try { await audio.current.play(); setSound(true); return true; }
    catch (_) { return false; }
  };

  useEffect(() => {
    let animation;
    let disposed = false;
    const awaken = () => { setAwake(true); startSound(); };
    window.addEventListener("pointerdown", awaken, { once: true, passive: true });
    window.addEventListener("keydown", awaken, { once: true });
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
        if (!disposed && holder.current) {
          animation = window.lottie.loadAnimation({
            container: holder.current, renderer: "svg", loop: true, autoplay: true, animationData: data,
          });
        }
      } catch (_) {
        if (holder.current) holder.current.innerHTML = '<span class="cat-fallback">◉ ◉</span>';
      }
    })();
    return () => { disposed = true; if (animation) animation.destroy(); if (audio.current) audio.current.pause(); };
  }, []);

  const toggleSound = async (event) => {
    event.stopPropagation();
    if (sound) {
      mutedByUser.current = true;
      if (audio.current) audio.current.pause();
      setSound(false);
      return;
    }
    mutedByUser.current = false;
    setAwake(true);
    await startSound();
  };

  return React.createElement("main", { className: "cat-engine", onPointerDown: startSound },
    React.createElement("style", null, `
      :root{--void:#050405;--panel:rgba(20,10,12,.54);--panel-deep:rgba(9,5,6,.78);--red:#e41c2d;--red-hot:#ff5361;--wine:#5d0c17;--paper:#f0e9e9;--ash:#95888c;--line:rgba(255,189,193,.16)}
      *{box-sizing:border-box}.cat-engine{isolation:isolate;position:relative;min-height:100svh;overflow:hidden;background:radial-gradient(ellipse 72% 61% at 50% 49%,#25090e 0%,#0d0708 43%,var(--void) 76%);color:var(--paper);font-family:Inter,ui-sans-serif,system-ui,-apple-system,"Segoe UI",sans-serif}.cat-engine:before{content:"";position:absolute;z-index:-1;inset:0;pointer-events:none;opacity:.075;background-image:repeating-linear-gradient(0deg,transparent 0,transparent 3px,#fff 4px),repeating-linear-gradient(90deg,transparent 0,transparent 95px,rgba(255,70,82,.4) 96px)}.cat-engine:after{content:"";position:absolute;z-index:-1;inset:-18%;pointer-events:none;background:radial-gradient(circle at center,transparent 0 16%,rgba(228,28,45,.1) 28%,transparent 48%),conic-gradient(from 30deg,transparent,rgba(228,28,45,.075),transparent 28%);animation:orbit 16s linear infinite}@keyframes orbit{to{transform:rotate(1turn)}}
      .glass{border:1px solid var(--line);background:linear-gradient(135deg,var(--panel),rgba(7,4,5,.28));box-shadow:inset 0 1px rgba(255,255,255,.055),0 18px 68px rgba(0,0,0,.28);backdrop-filter:blur(16px)}.topbar{position:absolute;z-index:3;top:max(16px,env(safe-area-inset-top));left:clamp(15px,3vw,34px);right:clamp(15px,3vw,34px);min-height:56px;padding:10px 13px;display:flex;align-items:center;justify-content:space-between}.brand{display:flex;gap:10px;align-items:center;font:700 10px/1 ui-monospace,SFMono-Regular,Menlo,monospace;letter-spacing:.24em}.brand-mark{position:relative;display:grid;place-items:center;width:30px;height:30px;border:1px solid rgba(255,83,97,.55);border-radius:50%;color:var(--red-hot);font-size:15px;box-shadow:0 0 16px rgba(228,28,45,.28)}.brand-mark:before{content:"";position:absolute;inset:5px;border:1px solid rgba(228,28,45,.5);border-radius:50%;animation:pulse 2.2s ease-in-out infinite}.brand small{display:block;margin-top:4px;color:var(--ash);font-size:6px;letter-spacing:.18em}.state{display:flex;align-items:center;gap:8px;color:#d8c6c8;font:8px/1 ui-monospace,SFMono-Regular,Menlo,monospace;letter-spacing:.15em}.state i{width:5px;height:5px;border-radius:50%;background:var(--red-hot);box-shadow:0 0 12px var(--red);animation:pulse 1.6s infinite}.sound{display:grid;place-items:center;width:35px;height:35px;border:1px solid var(--line);background:rgba(255,255,255,.025);color:var(--paper);font-size:13px}.sound[data-active="true"]{color:var(--red-hot);border-color:rgba(255,83,97,.62);box-shadow:inset 0 0 18px rgba(228,28,45,.18),0 0 15px rgba(228,28,45,.1)}.sound:active{transform:scale(.94)}
      .side-panel{position:absolute;z-index:2;left:clamp(15px,3vw,34px);top:98px;width:205px;padding:15px}.micro-label{color:var(--ash);font:7px/1 ui-monospace,SFMono-Regular,Menlo,monospace;letter-spacing:.21em}.protocol{display:grid;gap:10px;margin-top:17px}.protocol div{display:grid;grid-template-columns:17px 1fr;gap:8px;align-items:center;color:#d7c6c8;font:8px/1.2 ui-monospace,SFMono-Regular,Menlo,monospace;letter-spacing:.1em}.protocol span{color:#74565a}.protocol i{height:1px;background:linear-gradient(90deg,var(--red),transparent);box-shadow:0 0 8px var(--red)}.protocol b{font-weight:400}.protocol em{grid-column:2;color:var(--ash);font:7px/1.2 Inter,sans-serif;font-style:normal;letter-spacing:0}.telemetry{position:absolute;z-index:2;right:clamp(15px,3vw,34px);bottom:max(18px,env(safe-area-inset-bottom));width:220px;padding:15px}.telemetry-grid{display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-top:14px}.telemetry-grid article{min-height:76px;padding:9px;border:1px solid rgba(255,189,193,.1);background:rgba(0,0,0,.13)}.telemetry-grid span{display:block;color:var(--ash);font:6px/1.3 ui-monospace,SFMono-Regular,Menlo,monospace;letter-spacing:.1em}.telemetry-grid b{display:block;margin-top:11px;color:var(--paper);font:17px/1 ui-monospace,SFMono-Regular,Menlo,monospace;font-weight:400}.telemetry-grid small{color:var(--red-hot);font:6px ui-monospace,SFMono-Regular,Menlo,monospace}.telemetry-foot{margin-top:12px;color:var(--ash);font:7px/1.4 ui-monospace,SFMono-Regular,Menlo,monospace;letter-spacing:.07em}
      .center-stage{position:absolute;inset:0;display:grid;place-items:center;padding:92px 20px 100px}.center-stage:before{content:"";position:absolute;width:min(66vw,410px);aspect-ratio:1;border-radius:50%;background:radial-gradient(circle,rgba(255,54,70,.28),rgba(143,9,22,.16) 30%,rgba(34,5,9,.07) 55%,transparent 70%);filter:blur(3px);animation:breathe 3.8s ease-in-out infinite}.cat-wrap{position:relative;z-index:1;display:grid;place-items:center;width:min(52vw,260px);height:min(45svh,330px);filter:drop-shadow(0 0 28px rgba(228,28,45,.49))}.cat-wrap:before{content:"";position:absolute;z-index:-1;inset:7%;border:1px solid rgba(255,83,97,.22);border-radius:50%;box-shadow:inset 0 0 34px rgba(228,28,45,.14),0 0 50px rgba(228,28,45,.12);animation:spin 15s linear infinite}.cat-wrap:after{content:"";position:absolute;z-index:2;width:34%;height:7%;top:48%;background:radial-gradient(ellipse at 18% 50%,#fff 0 8%,var(--red-hot) 12% 28%,transparent 33%),radial-gradient(ellipse at 82% 50%,#fff 0 8%,var(--red-hot) 12% 28%,transparent 33%);filter:drop-shadow(0 0 8px var(--red))}.cat-wrap svg{position:absolute;inset:0;width:100%!important;height:100%!important;opacity:.72}.cat-fallback{color:var(--red-hot);font:24px ui-monospace;letter-spacing:18px;filter:drop-shadow(0 0 10px var(--red))}.crosshair{position:absolute;width:min(66vw,340px);aspect-ratio:1;border:1px solid rgba(255,189,193,.12);border-radius:50%;pointer-events:none}.crosshair:before,.crosshair:after{content:"";position:absolute;background:rgba(255,189,193,.16)}.crosshair:before{height:1px;left:-10%;right:-10%;top:50%}.crosshair:after{width:1px;top:-10%;bottom:-10%;left:50%}.hero-copy{position:absolute;z-index:2;left:clamp(18px,6vw,92px);bottom:max(24px,env(safe-area-inset-bottom));max-width:325px}.hero-copy p{margin:0 0 12px;color:var(--ash);font:8px/1.4 ui-monospace,SFMono-Regular,Menlo,monospace;letter-spacing:.15em}.hero-copy p i{display:inline-block;width:22px;height:1px;margin-right:9px;vertical-align:middle;background:var(--red);box-shadow:0 0 8px var(--red)}.hero-copy h1{margin:0;font-size:clamp(30px,4vw,54px);line-height:.91;letter-spacing:-.065em;font-weight:300;text-shadow:0 5px 34px #000}.hero-copy h1 em{font-style:normal;color:transparent;-webkit-text-stroke:1px rgba(255,174,179,.65);filter:drop-shadow(0 0 12px rgba(228,28,45,.3))}.corner{position:absolute;z-index:2;width:14px;height:14px;border-color:rgba(255,118,127,.42);border-style:solid;pointer-events:none}.tl{left:12px;top:12px;border-width:1px 0 0 1px}.tr{right:12px;top:12px;border-width:1px 1px 0 0}.bl{left:12px;bottom:12px;border-width:0 0 1px 1px}.br{right:12px;bottom:12px;border-width:0 1px 1px 0}.wake{position:absolute;z-index:4;left:50%;top:calc(max(16px,env(safe-area-inset-top)) + 73px);transform:translateX(-50%);padding:8px 12px;color:#d8bfc2;border:1px solid rgba(255,83,97,.26);background:rgba(30,7,10,.55);font:7px/1 ui-monospace,SFMono-Regular,Menlo,monospace;letter-spacing:.15em;opacity:0;transition:.3s;pointer-events:none}.wake.show{opacity:1}.wake i{color:var(--red-hot);font-style:normal}@keyframes pulse{50%{opacity:.35;transform:scale(.75)}}@keyframes spin{to{transform:rotate(1turn)}}@keyframes breathe{50%{transform:scale(1.1);opacity:.68}}@media(max-width:820px){.side-panel,.telemetry{display:none}.hero-copy{left:50%;bottom:calc(18px + env(safe-area-inset-bottom));transform:translateX(-50%);width:calc(100% - 42px);text-align:center}.cat-wrap{width:min(60vw,245px);height:min(44svh,300px)}.center-stage{padding-bottom:130px}.hero-copy h1{font-size:32px}.state span{display:none}}@media(prefers-reduced-motion:reduce){.cat-engine:after,.cat-wrap:before,.center-stage:before,.brand-mark:before,.state i{animation:none}}
    `),
    React.createElement("span", { className: "corner tl" }), React.createElement("span", { className: "corner tr" }), React.createElement("span", { className: "corner bl" }), React.createElement("span", { className: "corner br" }),
    React.createElement("header", { className: "topbar glass" },
      React.createElement("div", { className: "brand" }, React.createElement("span", { className: "brand-mark" }, "◈"), React.createElement("span", null, "NIGHTFALL", React.createElement("small", null, "PRIVATE NETWORK // SECURE CHANNEL"))),
      React.createElement("div", { className: "state" }, React.createElement("i", null), React.createElement("span", null, "VEIL ACTIVE")),
      React.createElement("button", { className: "sound", "data-active": sound, onClick: toggleSound, "aria-label": sound ? "Turn sound off" : "Turn sound on" }, sound ? "♫" : "◖))")
    ),
    React.createElement("aside", { className: "side-panel glass" },
      React.createElement("div", { className: "micro-label" }, "NIGHT PROTOCOL"),
      React.createElement("div", { className: "protocol" },
        React.createElement("div", null, React.createElement("span", null, "01"), React.createElement("i", null), React.createElement("b", null, "RED VEIL"), React.createElement("em", null, "Aura containment stable")),
        React.createElement("div", null, React.createElement("span", null, "02"), React.createElement("i", null), React.createElement("b", null, "EYES OPEN"), React.createElement("em", null, "Observer detected")),
        React.createElement("div", null, React.createElement("span", null, "03"), React.createElement("i", null), React.createElement("b", null, "CHANNEL"), React.createElement("em", null, "Encrypted and silent"))
      )
    ),
    React.createElement("section", { className: "center-stage", "aria-label": "Animated evil cat" },
      React.createElement("div", { className: "crosshair" }),
      React.createElement("div", { className: "cat-wrap", ref: holder })
    ),
    React.createElement("div", { className: "hero-copy" }, React.createElement("p", null, React.createElement("i", null), "PRIVATE NETWORK // HALLOWEEN MODE"), React.createElement("h1", null, "THE NIGHT IS\n", React.createElement("em", null, "WATCHING."))),
    React.createElement("aside", { className: "telemetry glass" },
      React.createElement("div", { className: "micro-label" }, "LIVE TELEMETRY"),
      React.createElement("div", { className: "telemetry-grid" },
        React.createElement("article", null, React.createElement("span", null, "AURA LEVEL"), React.createElement("b", null, awake ? "98" : "72"), React.createElement("small", null, "%")),
        React.createElement("article", null, React.createElement("span", null, "SIGNAL"), React.createElement("b", null, "0.91"), React.createElement("small", null, "Λ"))
      ),
      React.createElement("div", { className: "telemetry-foot" }, "OBSERVER // NO ECHO DETECTED")
    ),
    React.createElement("div", { className: "wake" + (awake ? " show" : "") }, React.createElement("i", null, "◆ "), sound ? "SONIC VEIL ENABLED" : "THE CAT IS AWAKE")
  );
}

window.App = App;
