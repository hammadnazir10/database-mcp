const nodemailer = require('nodemailer');

function createTransporter() {
  return nodemailer.createTransport({
    host: process.env.SMTP_HOST || 'smtp.gmail.com',
    port: parseInt(process.env.SMTP_PORT || '587'),
    secure: false,
    auth: {
      user: process.env.EMAIL_USER,
      pass: process.env.EMAIL_PASS
    }
  });
}

async function sendEscalationEmail({ sessionId, customerMessage, conversationHistory, timestamp }) {
  const transporter = createTransporter();

  const conversationText = conversationHistory
    .map(m => `[${m.role.toUpperCase()}]: ${m.content}`)
    .join('\n\n');

  const mailOptions = {
    from: process.env.EMAIL_USER,
    to: process.env.AGENT_EMAIL,
    subject: `[Escalation Required] Customer Support Session ${sessionId}`,
    text: `A customer has requested human support or expressed significant dissatisfaction.

SESSION ID: ${sessionId}
TIMESTAMP: ${timestamp}

TRIGGERING MESSAGE:
${customerMessage}

FULL CONVERSATION HISTORY:
${conversationText}

Please follow up with this customer as soon as possible.

---
This is an automated notification from the Customer Support AI Agent.`,
    html: `
<html>
<body style="font-family: Arial, sans-serif; max-width: 700px; margin: 0 auto; padding: 20px;">
  <div style="background: #dc3545; color: white; padding: 15px; border-radius: 6px 6px 0 0;">
    <h2 style="margin: 0;">⚠ Escalation Required</h2>
    <p style="margin: 5px 0 0;">Customer Support Session ${sessionId}</p>
  </div>
  <div style="border: 1px solid #dee2e6; border-top: none; padding: 20px; border-radius: 0 0 6px 6px;">
    <p><strong>Timestamp:</strong> ${timestamp}</p>
    <div style="background: #fff3cd; border: 1px solid #ffc107; padding: 12px; border-radius: 4px; margin: 15px 0;">
      <strong>Triggering Message:</strong><br>
      <em>${customerMessage}</em>
    </div>
    <h3>Conversation History</h3>
    <div style="background: #f8f9fa; padding: 15px; border-radius: 4px; white-space: pre-wrap; font-size: 14px;">
${conversationHistory.map(m => `<div style="margin-bottom: 10px;"><strong>${m.role === 'user' ? 'Customer' : 'AI Agent'}:</strong> ${m.content}</div>`).join('')}
    </div>
    <p style="color: #6c757d; font-size: 13px; margin-top: 20px;">
      This is an automated notification from the Customer Support AI Agent.
    </p>
  </div>
</body>
</html>`
  };

  await transporter.sendMail(mailOptions);
}

module.exports = { sendEscalationEmail };
