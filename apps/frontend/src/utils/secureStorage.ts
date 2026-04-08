export const secureStorage = {
  encrypt: async (unencrypted: string): Promise<string> => {
    if (!unencrypted || !window.secureStorage) return ""
    return await window.secureStorage.encrypt(unencrypted)
  },

  decrypt: async (encrypted: string): Promise<string> => {
    if (!encrypted || !window.secureStorage) return ""
    return await window.secureStorage.decrypt(encrypted)
  },

  isAvailable: (): boolean => {
    return window.secureStorage ? true : false
  },
}

declare global {
  interface Window {
    secureStorage?: {
      encrypt: (password: string) => Promise<string>
      decrypt: (encrypted: string) => Promise<string>
    }
  }
}
