const rawMap = {
    '&': '&amp;',
    '<': '&lt;',
    '>': '&gt;',
    '"': '&quot;',
    "'": '&#39;'
} as const;


/**
 * Escapes text for safe interpolation into HTML.
 */
export function escapeHTML(str: string) {
    return str.replace(/[&<>"']/g, (char) => rawMap[char as keyof typeof rawMap]);
}
