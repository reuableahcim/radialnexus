<template>
  <div id="colors-graph"></div>
</template>

<script>
import ForceGraph3D from '3d-force-graph';
import * as THREE from 'three';

function hexToRgb(hex) {
  hex = hex.replace(/^#/, '');
  const bigint = parseInt(hex, 16);
  const r = (bigint >> 16) & 255;
  const g = (bigint >> 8) & 255;
  const b = bigint & 255;
  return `rgb(${r}, ${g}, ${b})`;
}

export default {
  name: 'ColorsGraph',
  mounted() {
    fetch('domain/colors.json') // Note: Correct the domain path
      .then(res => res.json())
      .then(data => {
        const myGraph = ForceGraph3D()(this.$el);
        myGraph
          .graphData(data)
          .nodeThreeObject(node => {
            const obj = new THREE.Mesh(
              new THREE.SphereGeometry(12),
              new THREE.MeshBasicMaterial({ color: hexToRgb(node.color) })
            );
            return obj;
          })
          .linkColor(() => 'rgba(255, 255, 255, 0.5)')
          .linkOpacity(0.5);
      })
      .catch(err => console.error(err));
  }
};
</script>

<style scoped>
#colors-graph {
  width: 100%;
  height: 100vh;
}
</style>
