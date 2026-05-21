import { useState, useEffect, useRef } from 'react'
import ReactMarkdown from 'react-markdown'

type Message = { role: 'user' | 'assistant'; content: string; source?: string | null }

const OPENAI_MODELS = ['gpt-4o-mini', 'gpt-4o', 'gpt-4-turbo']
const GOOGLE_MODELS: { value: string; label: string }[] = [
  { value: 'gemini-3.1-flash-lite-preview', label: 'gemini-3.1-flash-lite-preview (Cost-Efficient/Fast)' },
  { value: 'gemini-3-flash-preview', label: 'gemini-3-flash-preview (Standard Performance)' },
  { value: 'gemini-3-deep-think', label: 'gemini-3-deep-think (Advanced Research/Math)' },
  { value: 'gemini-3.1-flash-image-preview', label: 'gemini-3.1-flash-image-preview (Image Generation)' },
  { value: 'gemini-2.5-pro', label: 'gemini-2.5-pro (Stable Production)' },
  { value: 'gemini-2.5-flash', label: 'gemini-2.5-flash (Stable Production)' },
]

function isEmbed(): boolean {
  if (typeof window === 'undefined') return false
  return new URLSearchParams(window.location.search).get('embed') === '1'
}

export default function App() {
  const embed = isEmbed()
  const [version, setVersion] = useState<string>('…')
  const [messages, setMessages] = useState<Message[]>([])
  const [input, setInput] = useState('')
  const [loading, setLoading] = useState(false)
  const [mode, setMode] = useState<'tool_only' | 'llm' | 'llm_only'>('llm_only')
  const [provider, setProvider] = useState<'OpenAI' | 'Google'>('Google')
  const [model, setModel] = useState('gemini-2.5-flash')
  const [apiKey, setApiKey] = useState('')
  const [settingsPath, setSettingsPath] = useState('')
  const [envApiKeys, setEnvApiKeys] = useState<{ openai: boolean; google: boolean }>({ openai: false, google: false })
  const [copiedId, setCopiedId] = useState<string | null>(null)
  const messagesEndRef = useRef<HTMLDivElement>(null)
  const abortControllerRef = useRef<AbortController | null>(null)

  useEffect(() => {
    const raf = requestAnimationFrame(() => {
      messagesEndRef.current?.scrollIntoView({ behavior: 'auto', block: 'end' })
    })
    return () => cancelAnimationFrame(raf)
  }, [messages, loading])

  const copyToClipboard = async (text: string, id: string) => {
    try {
      await navigator.clipboard.writeText(text)
      setCopiedId(id)
      setTimeout(() => setCopiedId(null), 2000)
    } catch (_) {}
  }

  const clearConversation = () => {
    setMessages([])
  }

  const copyConversation = () => {
    const text = messages
      .map((m) => `${m.role === 'user' ? 'You' : m.source ? `Assistant (${m.source})` : 'Assistant'}: ${m.content}`)
      .join('\n\n')
    copyToClipboard(text, 'conversation')
  }

  useEffect(() => {
    fetch('/api/chat/env-api-keys')
      .then((r) => r.json())
      .then((d) => setEnvApiKeys({ openai: !!d.openai, google: !!d.google }))
      .catch(() => {})
  }, [])

  useEffect(() => {
    if (embed) return
    fetch('/api/version')
      .then((r) => r.json())
      .then((d) => setVersion(d.version ?? '?'))
      .catch(() => setVersion('?'))
  }, [embed])

  useEffect(() => {
    if (!embed) return
    document.documentElement.classList.add('embed-chat-page')
    return () => { document.documentElement.classList.remove('embed-chat-page') }
  }, [embed])

  const modelList = provider === 'Google' ? GOOGLE_MODELS.map((o) => o.value) : OPENAI_MODELS
  useEffect(() => {
    setModel((m) => (modelList.includes(m) ? m : modelList[0]))
  }, [provider])

  const stopServer = () => {
    fetch('/api/shutdown', { method: 'POST' }).catch(() => {})
  }

  const stopTurn = () => {
    abortControllerRef.current?.abort()
  }

  const send = async () => {
    const text = input.trim()
    if (!text || loading) return
    setInput('')
    setMessages((prev) => [...prev, { role: 'user', content: text }])
    setLoading(true)
    const controller = new AbortController()
    abortControllerRef.current = controller
    // Drop trailing user messages that have no assistant reply (e.g. after stop), so the model only answers the current message
    let historyMessages = messages.map(({ role, content }) => ({ role, content }))
    while (historyMessages.length > 0 && historyMessages[historyMessages.length - 1].role === 'user') {
      historyMessages.pop()
    }
    const history = historyMessages
    const body = JSON.stringify({
      message: text,
      history,
      mode,
      settings_path: settingsPath || undefined,
      provider,
      api_key: apiKey || undefined,
      model: model || undefined,
    })
    try {
      const res = await fetch('/api/chat/stream', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body,
        signal: controller.signal,
      })
      if (!res.ok || !res.body) {
        const data = await res.json().catch(() => ({}))
        throw new Error((data as { error?: string }).error || `Request failed: ${res.status}`)
      }
      setMessages((prev) => [...prev, { role: 'assistant', content: '', source: undefined }])
      const reader = res.body.getReader()
      const decoder = new TextDecoder()
      let buffer = ''
      let finalReply = ''
      let finalSource = 'tool_only'
      let finalModel: string | null = null
      while (true) {
        const { done, value } = await reader.read()
        if (done) break
        buffer += decoder.decode(value, { stream: true })
        const lines = buffer.split('\n')
        buffer = lines.pop() ?? ''
        for (const line of lines) {
          if (line.startsWith('data: ')) {
            try {
              const data = JSON.parse(line.slice(6)) as { delta?: string; reply?: string; source?: string; model?: string | null; error?: string }
              if (data.error) {
                setMessages((prev) => {
                  const next = [...prev]
                  const last = next[next.length - 1]
                  if (last?.role === 'assistant') next[next.length - 1] = { ...last, content: data.error ?? 'Error' }
                  return next
                })
                break
              }
              if (data.delta !== undefined) {
                setMessages((prev) => {
                  const next = [...prev]
                  const last = next[next.length - 1]
                  if (last?.role === 'assistant') next[next.length - 1] = { ...last, content: last.content + data.delta }
                  return next
                })
              }
              if (data.reply !== undefined) {
                finalReply = data.reply
                finalSource = data.source ?? 'tool_only'
                finalModel = data.model ?? null
                setMessages((prev) => {
                  const next = [...prev]
                  const last = next[next.length - 1]
                  if (last?.role === 'assistant') {
                    const content = last.content || finalReply
                    const sourceLabel = finalSource === 'direct_lookup' ? 'Direct lookup' : finalSource === 'llm' ? (finalModel ? `LLM (${finalModel})` : 'LLM') : 'Tool only'
                    next[next.length - 1] = { ...last, content, source: sourceLabel }
                  }
                  return next
                })
              }
            } catch (_) {}
          }
        }
      }
    } catch (err) {
      const isAborted = err instanceof Error && err.name === 'AbortError'
      if (isAborted) {
        // Remove the partial assistant reply so the next request doesn't include it in history
        setMessages((prev) => {
          const next = [...prev]
          if (next.length > 0 && next[next.length - 1]?.role === 'assistant') next.pop()
          return next
        })
      } else {
        setMessages((prev) => [...prev, { role: 'assistant', content: `Error: ${err instanceof Error ? err.message : String(err)}` }])
      }
    } finally {
      abortControllerRef.current = null
      setLoading(false)
    }
  }

  const chatContent = (
    <>
      <div className="chat-main">
        <div className="chat-header-row">
          <h2 className="chat-section-title">Migration Chat</h2>
          {messages.length > 0 && (
            <div className="chat-actions">
              <button type="button" className="chat-action-btn" onClick={clearConversation} title="Clear conversation">
                Clear
              </button>
              <button type="button" className="chat-action-btn" onClick={copyConversation} title="Copy full conversation">
                {copiedId === 'conversation' ? 'Copied!' : 'Copy conversation'}
              </button>
            </div>
          )}
        </div>
        <div className="chat-messages">
          {messages.length === 0 && (
            <p style={{ color: '#94a3b8', margin: 0 }}>Send a message to start. Try &quot;validate connections&quot; or &quot;list source dashboards&quot;.</p>
          )}
          {messages.map((m, i) => (
            <div key={i} className={`chat-message ${m.role}`}>
              <div className="chat-message-header">
                <span className="role">{m.role === 'user' ? 'You' : m.source ? `Assistant (${m.source})` : 'Assistant'}</span>
                <button
                  type="button"
                  className="chat-message-copy"
                  onClick={() => copyToClipboard(m.content, `msg-${i}`)}
                  title="Copy message"
                  aria-label="Copy message"
                >
                  {copiedId === `msg-${i}` ? 'Copied!' : 'Copy'}
                </button>
              </div>
              {m.role === 'user' ? (
                <div>{m.content}</div>
              ) : (
                <div className="markdown-body">
                  <ReactMarkdown
                    components={{
                      a: ({ href, children, ...props }) => (
                        <a
                          href={href}
                          target="_blank"
                          rel="noopener noreferrer"
                          onClick={(e) => {
                            if (href?.startsWith('/') || href?.startsWith('http')) {
                              e.preventDefault();
                              window.open(href, '_blank', 'noopener,noreferrer');
                            }
                          }}
                          {...props}
                        >
                          {children}
                        </a>
                      ),
                    }}
                  >
                    {m.content}
                  </ReactMarkdown>
                </div>
              )}
            </div>
          ))}
          {loading && (
            <div className="chat-message assistant">
              <div className="chat-message-header">
                <span className="role">Assistant</span>
              </div>
              <div>Thinking…</div>
            </div>
          )}
          <div ref={messagesEndRef} aria-hidden="true" />
        </div>
        <div className="chat-input-row">
          <input
            type="text"
            placeholder="e.g. validate connections"
            value={input}
            onChange={(e) => setInput(e.target.value)}
            onKeyDown={(e) => e.key === 'Enter' && send()}
            disabled={loading}
          />
          <button
            type="button"
            onClick={loading ? stopTurn : send}
            className={loading ? 'chat-stop-btn' : ''}
            disabled={!loading && !input.trim()}
          >
            {loading ? 'Stop' : 'Send'}
          </button>
        </div>
      </div>
      <aside className="chat-options">
        <h3>Options</h3>
        <label>Mode</label>
        <select value={mode} onChange={(e) => setMode(e.target.value as typeof mode)}>
          <option value="tool_only">Tool only</option>
          <option value="llm">LLM</option>
          <option value="llm_only">LLM only</option>
        </select>
        <label>Provider</label>
        <select value={provider} onChange={(e) => setProvider(e.target.value as typeof provider)}>
          <option value="OpenAI">OpenAI</option>
          <option value="Google">Google</option>
        </select>
        <label>Model</label>
        <select value={model} onChange={(e) => setModel(e.target.value)}>
          {provider === 'Google'
            ? GOOGLE_MODELS.map((o) => (
                <option key={o.value} value={o.value}>{o.label}</option>
              ))
            : OPENAI_MODELS.map((m) => (
                <option key={m} value={m}>{m}</option>
              ))}
        </select>
        <label>API key (LLM mode)</label>
        <input type="password" value={apiKey} onChange={(e) => setApiKey(e.target.value)} placeholder="sk-..." />
        {!apiKey.trim() && envApiKeys[provider === 'Google' ? 'google' : 'openai'] && (
          <p className="chat-option-env-hint">Using API key from .env</p>
        )}
        <label>Settings path (optional)</label>
        <input type="text" value={settingsPath} onChange={(e) => setSettingsPath(e.target.value)} placeholder="Default if empty" />
      </aside>
    </>
  )

  if (embed) {
    return <div className="embed-chat-wrap">{chatContent}</div>
  }

  return (
    <>
      <div className="app-container">
        <header className="app-header">
          <div className="app-header-top">
            <div>
              <h1 className="app-title">Sisense Migration &amp; Merge</h1>
              <p className="app-subtitle">Configure migration settings or validate dashboards and widgets on a server.</p>
            </div>
            <div className="app-version-wrap">
              <span className="app-version">v{version}</span>
            </div>
          </div>
          <div className="app-nav-row">
            <nav className="app-nav" aria-label="Main">
              <a href="/#migration" className="app-nav-link">Migration &amp; Merge</a>
              <a href="/#validation" className="app-nav-link">Validation</a>
              <a href="/#servers" className="app-nav-link">Manage Servers</a>
              <a href="/chat/" className="app-nav-link app-nav-link-active">Chat</a>
            </nav>
            <button type="button" className="app-stop-btn" onClick={stopServer}>Stop Server</button>
          </div>
        </header>
      </div>
      <div className="app-container chat-container">
        {chatContent}
      </div>
    </>
  )
}
