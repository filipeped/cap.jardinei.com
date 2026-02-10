import { useEffect, useState } from "react";

export default function Home() {
  const [status, setStatus] = useState("⏳ Enviando evento de teste...");
  const [responseData, setResponseData] = useState<any>(null);
  const [timestamp, setTimestamp] = useState<string>("");

  const sendTestEvent = async () => {
    const now = new Date();
    setTimestamp(now.toLocaleString("pt-BR"));
    setStatus("⏳ Enviando evento de teste...");

    const event = {
      event_name: "Lead",
      event_time: Math.floor(Date.now() / 1000),
      action_source: "website",
      event_source_url: "https://www.jardinei.com",
      user_data: {
        external_id: "dec28dba1ef8f7a974d0daa5fb417e886d608ff870dea037176fafd3ef931045",
        client_ip_address: "177.155.123.123",
        client_user_agent: navigator.userAgent,
        fbp: "fb.1.1751360590432.213448171908285443",
        fbc: "fb.1.1751360590432.IwAR3T_Exemplo"
      },
      custom_data: {
        value: 900,
        currency: "BRL",
        content_name: "LeadFromForm",
        content_type: "form",
        content_category: "lead",
        diagnostic_mode: true,
        triggered_by: "manual_test"
      }
    };

    const payload = {
      data: [event]
    };

    try {
      const res = await fetch("/api/events", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload)
      });

      const json = await res.json();
      setResponseData(json);

      if (json.events_received) {
        setStatus("✅ Evento recebido com sucesso pela Meta via proxy.");
      } else if (json.error) {
        setStatus("❌ Erro retornado pela Meta.");
      } else {
        setStatus("⚠️ Evento enviado, mas sem confirmação clara da Meta.");
      }
    } catch (err) {
      console.error(err);
      setStatus("❌ Erro na conexão com o proxy.");
    }
  };

  useEffect(() => {
    sendTestEvent();
  }, []);

  return (
    <div style={{ fontFamily: "sans-serif", padding: "40px", maxWidth: "800px", margin: "0 auto" }}>
      <h2>🔍 Diagnóstico do Proxy CAPI</h2>
      <p><strong>Status:</strong> {status}</p>
      <p><strong>Horário:</strong> {timestamp}</p>

      <button
        onClick={sendTestEvent}
        style={{
          padding: "10px 20px",
          marginTop: "20px",
          backgroundColor: "#0070f3",
          color: "white",
          border: "none",
          borderRadius: "4px",
          cursor: "pointer"
        }}
      >
        🔄 Reenviar evento de teste
      </button>

      <h3 style={{ marginTop: "30px" }}>📦 Resposta completa:</h3>
      <pre
        style={{
          backgroundColor: "#f4f4f4",
          padding: "20px",
          borderRadius: "8px",
          maxHeight: "400px",
          overflowY: "auto",
          fontSize: "14px"
        }}
      >
        {responseData ? JSON.stringify(responseData, null, 2) : "Aguardando resposta..."}
      </pre>
    </div>
  );
}
