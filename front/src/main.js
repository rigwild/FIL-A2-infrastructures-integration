import Vue from 'vue'
import App from './App.vue'
import vuetify from './plugins/vuetify'
import { Promised } from 'vue-promised'
import HighchartsVue from 'highcharts-vue'

Vue.config.productionTip = false

Vue.component('Promised', Promised)
Vue.use(HighchartsVue)

new Vue({
  vuetify,
  render: function(h) {
    return h(App)
  }
}).$mount('#app')
