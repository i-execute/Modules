// Self-contained Halloween cover page for XRay WebSocket fallback.
// React and ReactDOM are provided by the local WebSocket helper wrapper.
const { useEffect, useRef, useState } = React;

const PUMPKIN_JSON = "https://raw.githubusercontent.com/i-execute/Modules/main/Storage/XRay/Animations/Pumpkin.json";
const THEME_MP3 = "https://raw.githubusercontent.com/i-execute/Modules/main/Storage/Media/Halloween_theme.mp3";

function App() {
  const holder = useRef(null);
  const audio = useRef(null);
  const [sound, setSound] = useState(false);

  useEffect(() => {
    let animation;
    let dead = false;
    const load = async () => {
      try {
        if (!window.lottie) {
          await new Promise((resolve, reject) => {
            const tag = document.createElement("script");
            tag.src = "https://cdnjs.cloudflare.com/ajax/libs/lottie-web/5.12.2/lottie.min.js";
            tag.onload = resolve;
            tag.onerror = reject;
            document.head.appendChild(tag);
          });
        }
        const response = await fetch(PUMPKIN_JSON);
        if (!response.ok) throw new Error("pumpkin_" + response.status);
        const data = await response.json();
        if (!dead && holder.current) {
          animation = window.lottie.loadAnimation({
            container: holder.current,
            renderer: "svg",
            loop: true,
            autoplay: true,
            animationData: data,
          });
        }
      } catch (_) {
        if (holder.current) holder.current.textContent = "🎃";
      }
    };
    load();
    return () => { dead = true; if (animation) animation.destroy(); };
  }, []);

  const toggleSound = async () => {
    if (!audio.current) {
      audio.current = new Audio(THEME_MP3);
      audio.current.loop = true;
      audio.current.volume = 0.22;
    }
    if (sound) {
      audio.current.pause();
      setSound(false);
    } else {
      try { await audio.current.play(); setSound(true); } catch (_) {}
    }
  };

  return React.createElement(
    "main", { className: "halloween" },
    React.createElement("div", { className: "mist" }),
    React.createElement("div", { className: "pumpkin", ref: holder }),
    React.createElement("div", { className: "copy" },
      React.createElement("span", null, "midnight channel"),
      React.createElement("small", null, "the lantern is awake")
    ),
    React.createElement("button", { className: "sound", onClick: toggleSound, "aria-label": "Toggle Halloween sound" }, sound ? "sound on" : "sound off")
  );
}

window.App = App;
