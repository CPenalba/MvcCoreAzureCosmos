using Microsoft.Azure.Cosmos;
using MvcCoreAzureCosmos.Models;

namespace MvcCoreAzureCosmos.Services
{
    public class ServiceCosmosDb
    {
        //DENTRO DE COSMOS TRABAJAMOS CON CLIENT Y CONTAINERS.
        //DENTRO DE LOS CONTAINERS ESTAN LOS ITEMS
        //DESDE EL CODIGO VAMOS A CREAR UN CONTAINER TAMBIEN

        //RECIBIREMOS DOS CLASES, UNA EL COSMOSCLIENT PARA TRABAJAR CON CONTAINERS
        //RECIBIREMOS UN CONTAINER QUE SERA NUESTRA TABLA
        private CosmosClient clientCosmos;
        private Container containerCosmos;

        public ServiceCosmosDb(CosmosClient client, Container container)
        {
            this.containerCosmos = container;
            this.clientCosmos = client;
        }

        //VAMOS A CREAR UN METODO PARA CREAR NUESTRA BASE DE DATOS 
        //Y DENTRO NUESTRO CONTAINER PARA LOS ITEMS
        public async Task CreateDatabaseAsync()
        {
            //CREAMOS PRIMERO LA BASE DE DATOS
            await this.clientCosmos.CreateDatabaseIfNotExistsAsync("vehiculoscosmos");

            //DENTRO DE ESTA BASE DE DATOS, CREAREMOS NUESTROS CONTAINER
            ContainerProperties properties = new ContainerProperties("containercoches", "/id");

            //CREAMOS EL CONTAINER DENTRO DE NUESTRA BBDD
            await this.clientCosmos.GetDatabase("vehiculoscosmos").CreateContainerIfNotExistsAsync(properties);
        }

        //METODO PARA INSERTAR ELEMENTOS DENTRO DE COSMOS
        public async Task InsertCocheAsync(Coche car)
        {
            //EN EL MOMENTO DE INSERTAR, COSMOS NO SABE ASIGNAR AUTOMATICMANETE SU PARTITION KEY
            //DEBEMOS DECIRSELO DE FORMA EXPLICITA
            await this.containerCosmos.CreateItemAsync<Coche>(car, new PartitionKey(car.Id));
        }

        public async Task<List<Coche>> GetCochesAsync()
        {
            //UNA BASE DE DATOS COSMOS NO SABE EL NUMERO DE REGISTROS REALES
            //DEBEMOS LEER UTILIZANDO UN BUCLE WHILE MIENTRAS QUE EXISTAN REGISTROS
            var query = this.containerCosmos.GetItemQueryIterator<Coche>();
            List<Coche> coches = new List<Coche>();
            while (query.HasMoreResults)
            {
                var results = await query.ReadNextAsync();
                //SON MULTILPLES COCHES LO QUE DEVUELVE, SE ALMACENAN DENTRO DE NUETSRA COLECCION A LA VEZ
                coches.AddRange(results);
            }
            return coches;
        }

        public async Task UpdateCocheAsync(Coche car)
        {
            //VOY A UTIIZAR UN METODO LLAMADO UPDERT
            //DICHO METODO, SI ENCUENTRA EL COCHE LO MODIFICA Y SI NO LO ENCUENTRA, LO INSERTA
            await this.containerCosmos.UpsertItemAsync<Coche>(car, new PartitionKey(car.Id));
        }

        public async Task DeleteCocheAsync(string id)
        {
            await this.containerCosmos.DeleteItemAsync<Coche>(id, new PartitionKey(id));
        }

        //METODO PARA BUSCAR UN COCHE POR SU ID
        public async Task<Coche> FindCocheAsync(string id)
        {
            ItemResponse<Coche> response = await this.containerCosmos.ReadItemAsync<Coche>(id, new PartitionKey(id));
            return response.Resource;
        }
    }
}
