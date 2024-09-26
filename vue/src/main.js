// Import Vue from the 'vue' package
import { createApp } from 'vue';
// Import VueRouter from the 'vue-router' package
import { createRouter, createWebHistory } from 'vue-router';
// Import App.vue component
import App from './App.vue';
// Import LandingPage.vue component
import LandingPage from './views/LandingPage.vue';
// Import TreesGraph.vue component
import TreesGraph from './views/TreesGraph.vue';
// Import ColorsGraph.vue component
import ColorsGraph from './views/ColorsGraph.vue';
// Import JourneysGraph.vue component
import JourneysGraph from './views/JourneysGraph.vue';
// Import Code.vue component
import CypherCode from './views/CypherCode.vue';

// Create a Vue Router instance
const router = createRouter({
  history: createWebHistory(),
  routes: [
    { path: '/', component: LandingPage },
    { path: '/trees', component: TreesGraph },
    { path: '/colors', component: ColorsGraph },
    { path: '/journeys', component: JourneysGraph },	  
    { path: '/codes', component: CypherCode }
  ]
});

// Create a Vue app instance
const app = createApp(App);

// Use Vue Router with the Vue app instance
app.use(router);

// Mount the Vue app to the DOM
app.mount('#app');
