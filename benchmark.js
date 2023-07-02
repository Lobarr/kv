import http from 'k6/http';
import { sleep } from 'k6';

export const options = {
  stages: [
    { duration: '30s', target: 100 },
    { duration: '30s', target: 500 },
    { duration: '30s', target: 1000 },
    { duration: '30s', target: 5000 },
    { duration: '30s', target: 10000 },
    { duration: '30s', target: 25000 },
  ],
};

function generateRandomString(size) {
  const characters = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
  let randomString = '';

  for (let i = 0; i < size; i++) {
    const randomIndex = Math.floor(Math.random() * characters.length);
    randomString += characters.charAt(randomIndex);
  }

  return randomString;
}

const keys = Array.from({ length: 10 }, () => generateRandomString(100));

export default function () {
  for (const key of keys) {
    const value = generateRandomString(1000000);

    const set_response = http.post(`http://localhost:9998/keys/${key}`, value);
    check(set_response, { 'set request suceeded': (r) => r.status == 200 });

    const get_response = http.get(`http://localhost:9998/keys/${key}`);
    check(get_response, { 'get request suceeded and value is accurate': (r) => r.status === 200 && r.body === value });

  }
}