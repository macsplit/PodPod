let currentPage = 1;
let currentFeedId = '';
let currentStatus = '';
let currentSearch = '';

const episodesContainer = document.getElementById('episodes-container');
const feedsList = document.getElementById('feeds-list');
const statusFilters = document.getElementById('status-filters');
const searchInput = document.getElementById('search-input');
const searchBtn = document.getElementById('search-btn');
const clearSearchBtn = document.getElementById('clear-search-btn');
const prevBtn = document.getElementById('prev-btn');
const nextBtn = document.getElementById('next-btn');
const pageInfo = document.getElementById('page-info');
const queueList = document.getElementById('queue-list');
const queueCount = document.getElementById('queue-count');
const audioPlayer = document.getElementById('audio-player');
const playingTitle = document.getElementById('playing-title');
const checkFeedsBtn = document.getElementById('check-feeds-btn');

// Initial load
document.addEventListener('DOMContentLoaded', () => {
    loadFeeds();
    loadEpisodes();
    startPollingQueue();
});

async function loadFeeds() {
    const res = await fetch('/api/feeds');
    const feeds = await res.json();
    
    feedsList.innerHTML = '<li class="active" data-id="">All Feeds</li>';
    feeds.forEach(feed => {
        const li = document.createElement('li');
        li.textContent = feed.title;
        li.dataset.id = feed.id;
        li.addEventListener('click', () => {
            document.querySelectorAll('#feeds-list li').forEach(el => el.classList.remove('active'));
            li.classList.add('active');
            
            // Immediate feedback: clear content area or show spinner
            episodesContainer.innerHTML = '<div class="spinner">Loading...</div>';
            
            currentFeedId = feed.id;
            currentPage = 1;
            loadEpisodes();
        });
        feedsList.appendChild(li);
    });

    feedsList.firstChild.addEventListener('click', () => {
        document.querySelectorAll('#feeds-list li').forEach(el => el.classList.remove('active'));
        feedsList.firstChild.classList.add('active');
        currentFeedId = '';
        currentPage = 1;
        loadEpisodes();
    });
}

async function loadEpisodes() {
    const params = new URLSearchParams({
        page: currentPage,
        feedId: currentFeedId,
        status: currentStatus,
        search: currentSearch
    });

    const res = await fetch(`/api/episodes?${params}`);
    const data = await res.json();

    episodesContainer.innerHTML = '';
    data.episodes.forEach(ep => {
        const card = document.createElement('div');
        card.className = 'episode-card';
        card.innerHTML = `
            <h4>${ep.title}</h4>
            <div class="episode-meta">📡 ${ep.feed_title} | 📅 ${new Date(ep.pub_date).toLocaleDateString()}</div>
            <div class="episode-desc">${ep.description || 'No description available.'}</div>
            ${ep.download_status === 'downloading' ? `
                <div class="progress-bar"><div class="progress-fill" style="width: ${ep.progress}%"></div></div>
            ` : ''}
            <div class="episode-actions">
                <span>${getStatusEmoji(ep.download_status)} ${ep.download_status}</span>
                ${ep.download_status === 'completed' ? `
                    <button class="play-btn" data-id="${ep.id}" data-title="${ep.title}">Play ▶</button>
                ` : ''}
            </div>
        `;
        episodesContainer.appendChild(card);
    });

    // Event listeners for play buttons
    document.querySelectorAll('.play-btn').forEach(btn => {
        btn.addEventListener('click', () => {
            playEpisode(btn.dataset.id, btn.dataset.title);
        });
    });

    pageInfo.textContent = `Page ${data.page} of ${Math.ceil(data.total / data.limit) || 1}`;
    prevBtn.disabled = currentPage === 1;
    nextBtn.disabled = currentPage >= Math.ceil(data.total / data.limit);
}

function getStatusEmoji(status) {
    switch (status) {
        case 'pending': return '⏳';
        case 'downloading': return '📥';
        case 'completed': return '✅';
        case 'failed': return '❌';
        default: return '❓';
    }
}

function playEpisode(id, title) {
    playingTitle.textContent = `Playing: ${title}`;
    audioPlayer.src = `/play/${id}`;
    audioPlayer.play();
}

// Filters & Pagination
statusFilters.querySelectorAll('button').forEach(btn => {
    btn.addEventListener('click', () => {
        statusFilters.querySelectorAll('button').forEach(b => b.classList.remove('active'));
        btn.classList.add('active');
        currentStatus = btn.dataset.status;
        currentPage = 1;
        loadEpisodes();
    });
});

// Search
function performSearch() {
    currentSearch = searchInput.value;
    currentPage = 1;
    loadEpisodes();
}

searchBtn.addEventListener('click', performSearch);
clearSearchBtn.addEventListener('click', () => {
    searchInput.value = '';
    currentSearch = '';
    currentPage = 1;
    loadEpisodes();
});

searchInput.addEventListener('keypress', (e) => {
    if (e.key === 'Enter') performSearch();
});

prevBtn.addEventListener('click', () => {
    if (currentPage > 1) {
        currentPage--;
        loadEpisodes();
    }
});

nextBtn.addEventListener('click', () => {
    currentPage++;
    loadEpisodes();
});

// Queue Polling
function startPollingQueue() {
    setInterval(async () => {
        const res = await fetch('/api/queue');
        const data = await res.json();
        
        queueCount.textContent = data.pending + data.downloading.length;
        
        queueList.innerHTML = '';
        data.downloading.forEach(ep => {
            const item = document.createElement('div');
            item.className = 'queue-item';
            item.innerHTML = `
                <strong>${ep.title}</strong><br>
                ${ep.feed_title} - ${ep.progress}%
                <div class="progress-bar"><div class="progress-fill" style="width: ${ep.progress}%"></div></div>
            `;
            queueList.appendChild(item);
        });

        if (data.downloading.length > 0) {
            // Refresh episodes list if something is downloading to update progress
            if (currentStatus === '' || currentStatus === 'downloading') {
                loadEpisodes();
            }
        }
    }, 10000);
}

checkFeedsBtn.addEventListener('click', async () => {
    checkFeedsBtn.disabled = true;
    checkFeedsBtn.textContent = 'Checking... 🔄';
    await fetch('/api/check', { method: 'POST' });
    setTimeout(() => {
        checkFeedsBtn.disabled = false;
        checkFeedsBtn.textContent = 'Check for New Episodes 🔄';
        loadEpisodes();
    }, 2000);
});
