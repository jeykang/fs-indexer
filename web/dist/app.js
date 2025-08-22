/**
 * File Search Web Application
 */

const API_URL = '/api';
let currentPage = 1;
let currentQuery = '';
let searchTimeout = null;

// Initialize the application
document.addEventListener('DOMContentLoaded', () => {
    initializeEventListeners();
    loadStats();
});

// Initialize event listeners
function initializeEventListeners() {
    const searchInput = document.getElementById('search-input');
    const searchBtn = document.getElementById('search-btn');
    const searchMode = document.getElementById('search-mode');
    const sortOrder = document.getElementById('sort-order');
    const pageSize = document.getElementById('page-size');
    const prevBtn = document.getElementById('prev-page');
    const nextBtn = document.getElementById('next-page');

    // Search events
    searchBtn.addEventListener('click', () => performSearch());
    searchInput.addEventListener('keypress', (e) => {
        if (e.key === 'Enter') {
            performSearch();
        }
    });

    // Live search with debouncing
    searchInput.addEventListener('input', (e) => {
        clearTimeout(searchTimeout);
        searchTimeout = setTimeout(() => {
            if (e.target.value !== currentQuery) {
                performSearch();
            }
        }, 500);
    });

    // Options change events
    searchMode.addEventListener('change', () => performSearch());
    sortOrder.addEventListener('change', () => performSearch());
    pageSize.addEventListener('change', () => {
        currentPage = 1;
        performSearch();
    });

    // Pagination events
    prevBtn.addEventListener('click', () => {
        if (currentPage > 1) {
            currentPage--;
            performSearch();
        }
    });

    nextBtn.addEventListener('click', () => {
        currentPage++;
        performSearch();
    });

    // Filter events
    document.getElementById('filter-dir').addEventListener('keypress', (e) => {
        if (e.key === 'Enter') performSearch();
    });
    document.getElementById('filter-ext').addEventListener('keypress', (e) => {
        if (e.key === 'Enter') performSearch();
    });
    document.getElementById('filter-size-min').addEventListener('keypress', (e) => {
        if (e.key === 'Enter') performSearch();
    });
    document.getElementById('filter-size-max').addEventListener('keypress', (e) => {
        if (e.key === 'Enter') performSearch();
    });
}

// Load index statistics
async function loadStats() {
    try {
        const response = await fetch(`${API_URL}/stats`);
        const data = await response.json();

        document.getElementById('total-files').textContent = 
            `📁 ${formatNumber(data.total_files)} files`;
        
        if (data.last_scan) {
            const date = new Date(data.last_scan * 1000);
            document.getElementById('last-scan').textContent = 
                `🕒 Last scan: ${formatDate(date)}`;
        }
    } catch (error) {
        console.error('Failed to load stats:', error);
    }
}

// Perform search
async function performSearch() {
    const query = document.getElementById('search-input').value;
    const mode = document.getElementById('search-mode').value;
    const sort = document.getElementById('sort-order').value;
    const perPage = document.getElementById('page-size').value;
    const dir = document.getElementById('filter-dir').value;
    const ext = document.getElementById('filter-ext').value;
    const sizeMin = document.getElementById('filter-size-min').value;
    const sizeMax = document.getElementById('filter-size-max').value;

    currentQuery = query;

    // Build query parameters
    const params = new URLSearchParams({
        page: currentPage,
        per_page: perPage,
        mode: mode,
        sort: sort
    });

    if (query) params.append('q', query);
    if (dir) params.append('dir', dir);
    if (ext) {
        ext.split(',').forEach(e => {
            if (e.trim()) params.append('ext', e.trim());
        });
    }
    if (sizeMin) params.append('size_min', sizeMin);
    if (sizeMax) params.append('size_max', sizeMax);

    // Show loading state
    showLoading();

    try {
        const response = await fetch(`${API_URL}/search?${params}`);
        const data = await response.json();
        displayResults(data);
    } catch (error) {
        showError('Search failed: ' + error.message);
    }
}

// Display search results
function displayResults(data) {
    const resultsDiv = document.getElementById('results');
    const resultsHeader = document.getElementById('results-header');
    const pagination = document.getElementById('pagination');

    // Show results header
    resultsHeader.style.display = 'flex';
    document.getElementById('result-count').textContent = 
        `Found ${formatNumber(data.total)} results`;
    document.getElementById('search-time').textContent = 
        `${data.took_ms}ms`;

    // Display results
    if (data.results.length === 0) {
        resultsDiv.innerHTML = '<div class="empty">No results found</div>';
        pagination.style.display = 'none';
        return;
    }

    resultsDiv.innerHTML = data.results.map(file => `
        <div class="result-item">
            <div class="result-path">${escapeHtml(file.path)}</div>
            <div class="result-meta">
                <span>📄 ${escapeHtml(file.basename)}</span>
                <span>📁 ${escapeHtml(file.dirpath)}</span>
                <span>💾 ${file.size_formatted}</span>
                <span>📅 ${file.mtime_formatted}</span>
                <button class="copy-btn" onclick="copyPath('${escapeHtml(file.path)}', this)">
                    📋 Copy
                </button>
            </div>
        </div>
    `).join('');

    // Update pagination
    if (data.total_pages > 1) {
        pagination.style.display = 'flex';
        document.getElementById('page-info').textContent = 
            `Page ${data.page} of ${data.total_pages}`;
        document.getElementById('prev-page').disabled = data.page === 1;
        document.getElementById('next-page').disabled = data.page === data.total_pages;
    } else {
        pagination.style.display = 'none';
    }
}

// Copy path to clipboard
async function copyPath(path, button) {
    try {
        await navigator.clipboard.writeText(path);
        button.textContent = '✓ Copied';
        button.classList.add('copied');
        setTimeout(() => {
            button.textContent = '📋 Copy';
            button.classList.remove('copied');
        }, 2000);
    } catch (error) {
        console.error('Failed to copy:', error);
    }
}

// Show loading state
function showLoading() {
    document.getElementById('results').innerHTML = 
        '<div class="loading">🔍 Searching...</div>';
    document.getElementById('results-header').style.display = 'none';
    document.getElementById('pagination').style.display = 'none';
}

// Show error message
function showError(message) {
    document.getElementById('results').innerHTML = 
        `<div class="error">❌ ${escapeHtml(message)}</div>`;
    document.getElementById('results-header').style.display = 'none';
    document.getElementById('pagination').style.display = 'none';
}

// Utility functions
function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

function formatNumber(num) {
    return new Intl.NumberFormat().format(num);
}

function formatDate(date) {
    return new Intl.DateTimeFormat('en-US', {
        dateStyle: 'short',
        timeStyle: 'short'
    }).format(date);
}