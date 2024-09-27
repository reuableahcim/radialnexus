# vue
## Setup

cd path/to/nexus/domain
vue create vue

Install 3D Force Graph Package:
Inside your vue directory, install the necessary package:

cd vue
npm install 3d-force-graph

Organize JSON Files:

Create a public/domain directory inside vue.
Copy domain.json over to vue/public/domain
Copy views/TreesGraph.vue to views/domainGraph.vue and edit
	id
	name
	fetch
Edit main.js
	import domainGraph.vue
	routes

cd radialnexus/vue/

Start the Development Server:
Inside the vue directory, start the Vue development server:

npm run serve
Access the app at http://localhost:8080.


## Project setup
```
npm install
```

### Compiles and hot-reloads for development
```
npm run serve
```

### Compiles and minifies for production
```
npm run build
```

### Lints and fixes files
```
npm run lint
```

### Customize configuration
See [Configuration Reference](https://cli.vuejs.org/config/).
