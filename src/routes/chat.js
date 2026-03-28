const express = require('express');
const { v4: uuidv4 } = require('uuid');
const { getChatResponse } = require('../services/aiService');
const { sendEscalationEmail } = require('../services/emailService');

const router = express.Router();

// In-memory session store (keyed by sessionId)
const sessions = new Map();

function getOrCreateSession(sessionId) {
  if (!sessions.has(sessionId)) {
    sessions.set(sessionId, {
      id: sessionId,
      history: [],
      escalated: false,
      createdAt: new Date().toISOString()
    });
  }
  return sessions.get(sessionId);
}

// POST /api/chat
router.post('/', async (req, res) => {
  const { message, sessionId } = req.body;

  if (!message || typeof message !== 'string' || message.trim().length === 0) {
    return res.status(400).json({ error: 'Message is required' });
  }
  if (message.length > 2000) {
    return res.status(400).json({ error: 'Message too long (max 2000 characters)' });
  }

  const sid = sessionId || uuidv4();
  const session = getOrCreateSession(sid);

  try {
    const { message: aiMessage, shouldEscalate } = await getChatResponse(
      session.history,
      message.trim()
    );

    // Update conversation history
    session.history.push({ role: 'user', content: message.trim() });
    session.history.push({ role: 'assistant', content: aiMessage });

    // Trigger escalation email if needed and not already escalated
    let escalated = false;
    if (shouldEscalate && !session.escalated) {
      session.escalated = true;
      escalated = true;

      // Send email asynchronously — don't block response
      sendEscalationEmail({
        sessionId: sid,
        customerMessage: message.trim(),
        conversationHistory: session.history,
        timestamp: new Date().toISOString()
      }).catch(err => {
        console.error('Failed to send escalation email:', err.message);
      });
    }

    return res.json({
      sessionId: sid,
      message: aiMessage,
      escalated,
      timestamp: new Date().toISOString()
    });
  } catch (err) {
    console.error('Chat error:', err.message);

    if (err.status === 429) {
      return res.status(429).json({ error: 'Rate limit reached. Please try again in a moment.' });
    }
    if (err.status === 401) {
      return res.status(500).json({ error: 'AI service configuration error.' });
    }

    return res.status(500).json({ error: 'An error occurred. Please try again.' });
  }
});

// GET /api/chat/session/:sessionId — retrieve session history
router.get('/session/:sessionId', (req, res) => {
  const session = sessions.get(req.params.sessionId);
  if (!session) {
    return res.status(404).json({ error: 'Session not found' });
  }
  return res.json({
    sessionId: session.id,
    history: session.history,
    escalated: session.escalated,
    createdAt: session.createdAt
  });
});

module.exports = router;
