import app from './app.js';

const PORT = 3000;

try {
  app.listen(PORT, () => {
    console.log(`Server starting at http://localhost:${PORT}`);
  });
} catch (err) {
  console.error('Error loading the server: ', err);
}