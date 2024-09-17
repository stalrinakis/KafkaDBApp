function fetchRedditPosts() {
document.getElementById("postTabs").setAttribute("style", "display: none;");


 

  const numberOfPosts = document.getElementById("numberOfPosts").value;
  const timeCreated = document.getElementById("timeCreated").value;
  const sortOption = document.getElementById("sortOption").value;
  const subreddit = document.getElementById("subreddit").value;
  const keyword1 = document.getElementById("keyword1").value;
  const keyword2 = document.getElementById("keyword2").value;
  const keyword3 = document.getElementById("keyword3").value;

  // Make AJAX request to the servlet using fetch API
  fetch(`/FirstApp/Rposts?numberOfPosts=${numberOfPosts}&timeCreated=${timeCreated}&sortOption=${sortOption}&subreddit=${subreddit}&keyword1=${keyword1}&keyword2=${keyword2}&keyword3=${keyword3}`)
    .then(response => {
      if (!response.ok) {
        throw new Error(`Error fetching data. Status: ${response.status}`);
      }
      return response.json();
    })
    .then(responseData => {
      // Access the data as needed
      const posts = responseData.posts;

      console.log(posts);

      // Display the fetched posts
      displayPosts(posts);
    })
    .catch(error => {
      console.error("Error fetching data:", error.message);
    });
}





function displayPosts(posts) {
  if (typeof posts === "string") {
    posts = JSON.parse(posts);
  }

  const jsonBody = document.getElementById("jsonBody");

  if (!jsonBody) {
    console.error("Element with id 'jsonBody' not found.");
    return;
  }

  if (posts.length === 0) {
    jsonBody.innerHTML = "<tr><td colspan='13'>No posts found.</td></tr>";
    return;
  }

  const existingTable = document.getElementById("jsonDataTable");

  // Clear existing content in the table body
  existingTable.querySelector("tbody").innerHTML = "";
  

  posts.forEach((post, index) => {
    const tbody = existingTable.querySelector("tbody");

    const tr = document.createElement("tr");

    // Use the correct property names based on your Post class
    tr.innerHTML = `
      <td>${index + 1}</td>
      <td>${post.title}</td>
      <td>${post.id}</td>
      <td>${post.subreddit}</td>
      <td>${post.selftext}</td>
      <td>${post.author}</td>
      <td>${post.score}</td>
      <td>${post.num_comments}</td>
      <td>${post.is_video}</td>
      <td>${post.upvote_ratio}</td>
      <td>${post.subreddit_subscribers}</td>
      <td>${post.url}</td>
    `;

    tbody.appendChild(tr);
  });
  
   document.getElementById("postTabs").removeAttribute("style", "display: none;");

  showTab("JsonData");
  
}

// script.js

// Function to show/hide tabs based on the selected tab
function showTab(tabId) {
  // Hide all tab contents
  var tabContents = document.getElementsByClassName("tab-content");
  for (var i = 0; i < tabContents.length; i++) {
    tabContents[i].style.display = "none";
  }

  // Show the selected tab content
  var selectedTab = document.getElementById(tabId);
  if (selectedTab) {
    selectedTab.style.display = "block";
  }
}

// Add click event listeners to tab elements
document.addEventListener("DOMContentLoaded", function () {
  var tabs = document.getElementsByClassName("tab");
  for (var i = 0; i < tabs.length; i++) {
    tabs[i].addEventListener("click", function () {
      var tabId = this.getAttribute("onclick").match(/showTab\('(.*)'\)/)[1];
      showTab(tabId);
    });
  }
});
