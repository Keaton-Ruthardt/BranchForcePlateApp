const jwt = require('jsonwebtoken');

const USERNAME = 'admin';
const PASSWORD = 'password123';
const JWT_SECRET = 'supersecretjwtkey'; // In production, use env var

// Login handler
function login(req, res) {
  const { username, password } = req.body;
  if (username === USERNAME && password === PASSWORD) {
    // Issue JWT
    const token = jwt.sign({ username }, JWT_SECRET, { expiresIn: '8h' });
    return res.json({ token });
  }
  return res.status(401).json({ error: 'Invalid credentials' });
}

// Middleware to protect routes
function authenticateJWT(req, res, next) {
  const authHeader = req.headers.authorization;
  if (authHeader && authHeader.startsWith('Bearer ')) {
    const token = authHeader.split(' ')[1];
    jwt.verify(token, JWT_SECRET, (err, user) => {
      if (err) return res.status(403).json({ error: 'Invalid token' });
      req.user = user;
      next();
    });
  } else {
    res.status(401).json({ error: 'No token provided' });
  }
}

module.exports = { login, authenticateJWT }; 