const request = require('supertest');
const app = require('../src/index');

// Mock OpenAI to avoid real API calls in tests
jest.mock('openai', () => {
  return jest.fn().mockImplementation(() => ({
    chat: {
      completions: {
        create: jest.fn().mockResolvedValue({
          choices: [{ message: { content: 'Hello! How can I help you today?' } }],
          usage: { total_tokens: 50 }
        })
      }
    }
  }));
});

// Mock nodemailer
jest.mock('nodemailer', () => ({
  createTransport: jest.fn().mockReturnValue({
    sendMail: jest.fn().mockResolvedValue({ messageId: 'test-id' })
  })
}));

describe('POST /api/chat', () => {
  it('returns 400 for missing message', async () => {
    const res = await request(app).post('/api/chat').send({});
    expect(res.status).toBe(400);
    expect(res.body.error).toBeDefined();
  });

  it('returns 400 for empty message', async () => {
    const res = await request(app).post('/api/chat').send({ message: '   ' });
    expect(res.status).toBe(400);
  });

  it('returns 400 for message exceeding 2000 chars', async () => {
    const res = await request(app).post('/api/chat').send({ message: 'a'.repeat(2001) });
    expect(res.status).toBe(400);
  });

  it('returns a chat response with sessionId', async () => {
    const res = await request(app)
      .post('/api/chat')
      .send({ message: 'What are your business hours?' });

    expect(res.status).toBe(200);
    expect(res.body.message).toBeDefined();
    expect(res.body.sessionId).toBeDefined();
    expect(typeof res.body.sessionId).toBe('string');
  });

  it('maintains session across requests', async () => {
    const first = await request(app)
      .post('/api/chat')
      .send({ message: 'Hello' });

    const { sessionId } = first.body;

    const second = await request(app)
      .post('/api/chat')
      .send({ message: 'What are your hours?', sessionId });

    expect(second.body.sessionId).toBe(sessionId);
  });

  it('generates new sessionId when none provided', async () => {
    const res = await request(app)
      .post('/api/chat')
      .send({ message: 'Test message' });

    expect(res.body.sessionId).toBeDefined();
    expect(res.body.sessionId.length).toBeGreaterThan(0);
  });
});

describe('GET /api/chat/session/:sessionId', () => {
  it('returns 404 for unknown session', async () => {
    const res = await request(app).get('/api/chat/session/nonexistent-id');
    expect(res.status).toBe(404);
  });

  it('returns session history after conversation', async () => {
    const chatRes = await request(app)
      .post('/api/chat')
      .send({ message: 'Hello there' });

    const { sessionId } = chatRes.body;
    const histRes = await request(app).get(`/api/chat/session/${sessionId}`);

    expect(histRes.status).toBe(200);
    expect(histRes.body.history).toBeDefined();
    expect(Array.isArray(histRes.body.history)).toBe(true);
    expect(histRes.body.history.length).toBeGreaterThan(0);
  });
});

describe('GET /api/health', () => {
  it('returns ok status', async () => {
    const res = await request(app).get('/api/health');
    expect(res.status).toBe(200);
    expect(res.body.status).toBe('ok');
  });
});

describe('Escalation detection', () => {
  it('sets escalated=true when AI returns [ESCALATE] tag', async () => {
    const OpenAI = require('openai');
    OpenAI.mockImplementation(() => ({
      chat: {
        completions: {
          create: jest.fn().mockResolvedValue({
            choices: [{ message: { content: 'I understand your frustration. [ESCALATE] A human agent will be in touch.' } }],
            usage: { total_tokens: 40 }
          })
        }
      }
    }));

    const res = await request(app)
      .post('/api/chat')
      .send({ message: 'I want to speak to a manager now!' });

    expect(res.status).toBe(200);
    expect(res.body.escalated).toBe(true);
    // [ESCALATE] tag should be stripped from the response
    expect(res.body.message).not.toContain('[ESCALATE]');
  });
});
